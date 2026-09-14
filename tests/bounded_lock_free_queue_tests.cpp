#include "event_dispatcher/dispatcher.hpp"
#include "event_dispatcher/queue/bounded_lock_free_queue.hpp"
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

using event_dispatcher::queue::bounded_lock_free_queue;
using event_dispatcher::queue::queue_status;
using namespace std::chrono_literals;

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

template <typename Future> decltype(auto) get_ready(Future& future) {
    expect(future.wait_for(2s) == std::future_status::ready, "timed out waiting for test thread");
    return future.get();
}

void test_capacity_contract_and_fifo() {
    for (const std::size_t invalid : {0U, 1U, 3U, 6U}) {
        bool rejected = false;
        try {
            bounded_lock_free_queue<int> queue{invalid};
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        expect(rejected, "non-power-of-two or tiny capacity was accepted");
    }

    bounded_lock_free_queue<int> queue{2U};
    static_assert(event_dispatcher::queue::concurrent_queue<decltype(queue), int>);
    expect(queue.capacity() == 2U, "capacity mismatch");
    expect(queue.data_path_is_lock_free(), "required target atomics are not lock-free");
    expect(queue.try_pop().status() == queue_status::empty, "new queue is not empty");
    expect(queue.try_push(10) == queue_status::success, "first push failed");
    expect(queue.try_push(20) == queue_status::success, "second push failed");
    expect(queue.try_push(30) == queue_status::full, "full queue accepted an event");
    expect(queue.size() == 2U, "full queue size mismatch");
    expect(queue.try_pop().value() == 10, "first FIFO value mismatch");
    expect(queue.try_pop().value() == 20, "second FIFO value mismatch");
}

void test_move_only_retry_and_wraparound() {
    bounded_lock_free_queue<std::unique_ptr<int>> move_queue{2U};
    auto first = std::make_unique<int>(7);
    auto second = std::make_unique<int>(8);
    auto retryable = std::make_unique<int>(9);
    expect(move_queue.try_push(std::move(first)) == queue_status::success, "move push failed");
    expect(move_queue.try_push(std::move(second)) == queue_status::success, "move push failed");
    expect(move_queue.try_push(std::move(retryable)) == queue_status::full,
           "full move queue status mismatch");
    expect(retryable != nullptr, "failed push consumed a move-only event");
    expect(*move_queue.try_pop().value() == 7, "move-only value mismatch");

    bounded_lock_free_queue<std::size_t> tiny{2U};
    constexpr std::size_t cycles = 100'000U;
    for (std::size_t value = 0U; value < cycles; ++value) {
        expect(tiny.try_push(value) == queue_status::success, "tiny ring push failed");
        const auto result = tiny.try_pop();
        expect(result && result.value() == value, "tiny ring wraparound corrupted FIFO data");
    }
}

struct throwing_copy_event {
    explicit throwing_copy_event(int event_value) : value(event_value) {}

    throwing_copy_event(const throwing_copy_event& other) {
        if (throw_on_copy.exchange(false, std::memory_order_relaxed)) {
            throw std::runtime_error{"requested copy failure"};
        }
        value = other.value;
    }
    throwing_copy_event(throwing_copy_event&&) noexcept = default;

    inline static std::atomic<bool> throw_on_copy{false};
    int value;
};

struct counted_event {
    explicit counted_event(int event_value) : value(event_value) {
        alive.fetch_add(1, std::memory_order_relaxed);
    }
    counted_event(const counted_event&) = delete;
    counted_event(counted_event&& other) noexcept : value(other.value) {
        alive.fetch_add(1, std::memory_order_relaxed);
    }
    ~counted_event() { alive.fetch_sub(1, std::memory_order_relaxed); }

    inline static std::atomic<int> alive{0};
    int value;
};

void test_throwing_copy_retires_reservation() {
    bounded_lock_free_queue<throwing_copy_event> queue{2U};
    const throwing_copy_event failing{17};
    throwing_copy_event::throw_on_copy.store(true, std::memory_order_relaxed);

    bool threw = false;
    try {
        static_cast<void>(queue.try_push(failing));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    expect(threw, "requested event copy did not throw");
    expect(queue.size() == 0U, "cancelled reservation changed logical size");

    const throwing_copy_event recovered{23};
    expect(queue.try_push(recovered) == queue_status::success,
           "queue did not recover after a throwing copy");
    const auto result = queue.try_pop();
    expect(result && result.value().value == 23, "recovered event was corrupted");
    expect(queue.try_pop().status() == queue_status::empty, "cancelled cell became an event");
}

void test_close_drain_and_discard() {
    {
        bounded_lock_free_queue<int> queue{4U};
        expect(queue.try_push(1) == queue_status::success, "drain setup failed");
        expect(queue.try_push(2) == queue_status::success, "drain setup failed");
        queue.close();
        queue.close();
        expect(queue.closed(), "queue did not close");
        expect(queue.try_push(3) == queue_status::closed, "closed queue accepted an event");
        expect(queue.wait_pop().value() == 1, "first drain value mismatch");
        expect(queue.wait_pop().value() == 2, "second drain value mismatch");
        expect(queue.wait_pop().status() == queue_status::closed,
               "drained queue did not remain closed");
    }

    {
        bounded_lock_free_queue<std::shared_ptr<int>> queue{4U};
        auto first = std::make_shared<int>(1);
        auto second = std::make_shared<int>(2);
        std::weak_ptr<int> first_lifetime = first;
        std::weak_ptr<int> second_lifetime = second;
        expect(queue.try_push(std::move(first)) == queue_status::success, "discard setup failed");
        expect(queue.try_push(std::move(second)) == queue_status::success, "discard setup failed");
        expect(queue.close_and_discard() == 2U, "discard count mismatch");
        expect(queue.close_and_discard() == 0U, "repeated discard was not idempotent");
        expect(first_lifetime.expired() && second_lifetime.expired(),
               "discard did not destroy queued events");
        expect(queue.try_pop().status() == queue_status::closed,
               "discarded queue did not remain closed");
    }

    {
        counted_event::alive.store(0, std::memory_order_relaxed);
        bounded_lock_free_queue<counted_event> queue{4U};
        expect(queue.try_push(counted_event{1}) == queue_status::success,
               "first lifetime push failed");
        expect(queue.try_push(counted_event{2}) == queue_status::success,
               "second lifetime push failed");
        expect(counted_event::alive.load(std::memory_order_relaxed) == 2,
               "queue does not own exactly two live events");
        expect(queue.close_and_discard() == 2U, "lifetime discard count mismatch");
        expect(counted_event::alive.load(std::memory_order_relaxed) == 0,
               "discard did not destroy each event exactly once");
    }
}

void test_wait_timeout_stop_and_wakeup() {
    bounded_lock_free_queue<int> queue{2U};
    expect(queue.try_push(1) == queue_status::success, "wait setup failed");
    expect(queue.try_push(2) == queue_status::success, "wait setup failed");
    expect(queue.wait_push_until(3, std::chrono::steady_clock::now() + 10ms) ==
               queue_status::timeout,
           "timed push did not time out");

    std::promise<queue_status> pushed;
    auto pushed_future = pushed.get_future();
    std::thread producer{[&] { pushed.set_value(queue.wait_push(3)); }};
    expect(queue.try_pop().value() == 1, "capacity-producing pop failed");
    expect(get_ready(pushed_future) == queue_status::success, "waiting producer did not wake");
    producer.join();

    bounded_lock_free_queue<int> empty{2U};
    std::stop_source stop;
    std::promise<queue_status> popped;
    auto popped_future = popped.get_future();
    std::thread consumer{[&] { popped.set_value(empty.wait_pop(stop.get_token()).status()); }};
    stop.request_stop();
    expect(get_ready(popped_future) == queue_status::stopped,
           "waiting consumer did not observe cancellation");
    consumer.join();

    bounded_lock_free_queue<int> full{2U};
    expect(full.try_push(1) == queue_status::success, "producer-stop setup failed");
    expect(full.try_push(2) == queue_status::success, "producer-stop setup failed");
    std::stop_source producer_stop;
    std::promise<queue_status> stopped_push;
    auto stopped_push_future = stopped_push.get_future();
    std::thread stopped_producer{
        [&] { stopped_push.set_value(full.wait_push(3, producer_stop.get_token())); }};
    producer_stop.request_stop();
    expect(get_ready(stopped_push_future) == queue_status::stopped,
           "waiting producer did not observe cancellation");
    stopped_producer.join();

    std::promise<queue_status> closed_push;
    auto closed_push_future = closed_push.get_future();
    std::thread closed_producer{[&] { closed_push.set_value(full.wait_push(4)); }};
    full.close();
    expect(get_ready(closed_push_future) == queue_status::closed,
           "close did not wake a blocked producer");
    closed_producer.join();

    std::stop_source already_stopped;
    already_stopped.request_stop();
    const auto expired = std::chrono::steady_clock::now() - 1ms;
    expect(empty.wait_push_until(7, expired, already_stopped.get_token()) == queue_status::success,
           "immediate capacity did not win over timeout and stop");
    expect(empty.wait_pop(already_stopped.get_token()).value() == 7,
           "immediate event did not win over stop");
}

void test_discard_during_concurrent_traffic() {
    constexpr std::size_t rounds = 20U;
    for (std::size_t round = 0U; round < rounds; ++round) {
        bounded_lock_free_queue<std::size_t> queue{16U};
        std::barrier start{5};
        std::atomic<std::size_t> accepted{0U};
        std::atomic<std::size_t> consumed{0U};
        std::vector<std::thread> threads;

        for (std::size_t index = 0U; index < 2U; ++index) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                while (queue.wait_pop().status() == queue_status::success) {
                    consumed.fetch_add(1U, std::memory_order_relaxed);
                }
            });
            threads.emplace_back([&] {
                start.arrive_and_wait();
                std::size_t value = 0U;
                while (queue.wait_push(value++) == queue_status::success) {
                    accepted.fetch_add(1U, std::memory_order_relaxed);
                }
            });
        }

        start.arrive_and_wait();
        for (std::size_t spin = 0U; spin < 100U; ++spin) {
            std::this_thread::yield();
        }
        const auto discarded = queue.close_and_discard();
        for (auto& thread : threads) {
            thread.join();
        }
        expect(accepted.load(std::memory_order_relaxed) ==
                   consumed.load(std::memory_order_relaxed) + discarded,
               "discard traffic accounting mismatch");
        expect(queue.size() == 0U, "discard left a logical event behind");
    }
}

void run_concurrent_accounting(std::size_t producers, std::size_t consumers,
                               std::size_t events_per_producer) {
    const auto total = producers * events_per_producer;
    bounded_lock_free_queue<std::size_t> queue{64U};
    std::vector<std::atomic<unsigned int>> seen(total);
    std::atomic<std::size_t> consumed{0U};
    std::vector<std::thread> producer_threads;
    std::vector<std::thread> consumer_threads;

    for (std::size_t index = 0U; index < consumers; ++index) {
        consumer_threads.emplace_back([&] {
            while (true) {
                auto result = queue.wait_pop();
                if (result.status() == queue_status::closed) {
                    return;
                }
                expect(result.value() < total, "consumer received an out-of-range event");
                seen[result.value()].fetch_add(1U, std::memory_order_relaxed);
                consumed.fetch_add(1U, std::memory_order_relaxed);
            }
        });
    }

    for (std::size_t producer = 0U; producer < producers; ++producer) {
        producer_threads.emplace_back([&, producer] {
            const auto begin = producer * events_per_producer;
            const auto end = begin + events_per_producer;
            for (auto value = begin; value < end; ++value) {
                expect(queue.wait_push(value) == queue_status::success,
                       "concurrent producer failed before close");
            }
        });
    }

    for (auto& producer : producer_threads) {
        producer.join();
    }
    queue.close();
    for (auto& consumer : consumer_threads) {
        consumer.join();
    }
    expect(consumed.load(std::memory_order_relaxed) == total, "concurrent count mismatch");
    for (const auto& count : seen) {
        expect(count.load(std::memory_order_relaxed) == 1U, "event was lost or duplicated");
    }
}

void test_concurrent_topologies_and_close() {
    run_concurrent_accounting(4U, 1U, 5'000U);
    run_concurrent_accounting(1U, 4U, 5'000U);
    run_concurrent_accounting(4U, 4U, 5'000U);

    constexpr std::size_t rounds = 30U;
    for (std::size_t round = 0U; round < rounds; ++round) {
        bounded_lock_free_queue<std::size_t> queue{8U};
        std::barrier start{5};
        std::atomic<std::size_t> accepted{0U};
        std::atomic<std::size_t> consumed{0U};
        std::vector<std::thread> threads;
        for (std::size_t index = 0U; index < 2U; ++index) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                while (queue.wait_pop().status() == queue_status::success) {
                    consumed.fetch_add(1U, std::memory_order_relaxed);
                }
            });
            threads.emplace_back([&] {
                start.arrive_and_wait();
                std::size_t value = 0U;
                while (queue.wait_push(value++) == queue_status::success) {
                    accepted.fetch_add(1U, std::memory_order_relaxed);
                }
            });
        }
        start.arrive_and_wait();
        for (std::size_t spin = 0U; spin < 100U; ++spin) {
            std::this_thread::yield();
        }
        queue.close();
        for (auto& thread : threads) {
            thread.join();
        }
        expect(accepted.load(std::memory_order_relaxed) == consumed.load(std::memory_order_relaxed),
               "close lost an accepted event");
    }
}

void test_dispatcher_policy_integration() {
    event_dispatcher::dispatcher_config config;
    config.queue_capacity = 64U;
    config.worker_count = 2U;
    event_dispatcher::dispatcher<int, bounded_lock_free_queue> dispatcher{config};
    std::atomic<std::size_t> delivered{0U};
    auto subscription = dispatcher.subscribe(
        [&](const int&) { delivered.fetch_add(1U, std::memory_order_relaxed); });
    for (int event = 0; event < 1'000; ++event) {
        expect(dispatcher.publish(event) == queue_status::success,
               "lock-free dispatcher publication failed");
    }
    dispatcher.shutdown();
    expect(delivered.load(std::memory_order_relaxed) == 1'000U,
           "lock-free dispatcher delivery count mismatch");
}

} // namespace

int main() {
    test_capacity_contract_and_fifo();
    test_move_only_retry_and_wraparound();
    test_throwing_copy_retires_reservation();
    test_close_drain_and_discard();
    test_wait_timeout_stop_and_wakeup();
    test_concurrent_topologies_and_close();
    test_discard_during_concurrent_traffic();
    test_dispatcher_policy_integration();
}
