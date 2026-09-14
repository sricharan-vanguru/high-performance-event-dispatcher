#include "event_dispatcher/queue/bounded_lock_free_queue.hpp"
#include "event_dispatcher/queue/bounded_mutex_queue.hpp"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
using event_dispatcher::queue::bounded_lock_free_queue;
using event_dispatcher::queue::bounded_mutex_queue;
using event_dispatcher::queue::queue_status;

struct benchmark_case {
    std::string_view name;
    std::size_t producers;
    std::size_t consumers;
};

struct benchmark_event {
    std::uint64_t sequence;
};

[[nodiscard]] std::int64_t percentile(const std::vector<std::int64_t>& sorted, double fraction) {
    const auto position =
        static_cast<std::size_t>(std::ceil(fraction * static_cast<double>(sorted.size())) - 1.0);
    return sorted[std::min(position, sorted.size() - 1U)];
}

template <template <typename> class Queue>
void run_case(std::string_view queue_name, const benchmark_case& config, std::size_t total_events) {
    constexpr std::size_t queue_capacity = 1'024U;
    Queue<benchmark_event> queue{queue_capacity};

    std::barrier start_line{static_cast<std::ptrdiff_t>(config.producers + config.consumers + 1U)};
    std::atomic<std::size_t> consumed{0U};
    std::vector<std::vector<std::int64_t>> producer_latencies(config.producers);
    std::vector<std::thread> consumers;
    std::vector<std::thread> producers;

    consumers.reserve(config.consumers);
    for (std::size_t index = 0; index < config.consumers; ++index) {
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

    producers.reserve(config.producers);
    const std::size_t per_producer = total_events / config.producers;
    for (std::size_t producer = 0; producer < config.producers; ++producer) {
        producers.emplace_back([&, producer] {
            auto& latencies = producer_latencies[producer];
            latencies.reserve(per_producer);
            start_line.arrive_and_wait();

            for (std::size_t index = 0; index < per_producer; ++index) {
                benchmark_event event{
                    static_cast<std::uint64_t>((producer * per_producer) + index)};
                const auto begin = clock_type::now();
                const auto status = queue.wait_push(std::move(event));
                const auto end = clock_type::now();
                if (status != queue_status::success) {
                    throw std::runtime_error{"benchmark publication failed"};
                }
                latencies.push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
            }
        });
    }

    start_line.arrive_and_wait();
    const auto begin = clock_type::now();
    for (auto& producer : producers) {
        producer.join();
    }
    queue.close();
    for (auto& consumer : consumers) {
        consumer.join();
    }
    const auto end = clock_type::now();

    std::vector<std::int64_t> latencies;
    latencies.reserve(per_producer * config.producers);
    for (auto& producer_values : producer_latencies) {
        latencies.insert(latencies.end(), producer_values.begin(), producer_values.end());
    }
    std::sort(latencies.begin(), latencies.end());

    const std::size_t published = per_producer * config.producers;
    if (consumed.load(std::memory_order_relaxed) != published || latencies.empty()) {
        throw std::runtime_error{"benchmark event accounting failed"};
    }

    const double seconds = std::chrono::duration<double>(end - begin).count();
    const double events_per_second = static_cast<double>(published) / seconds;

    std::cout << std::left << std::setw(12) << queue_name << std::setw(6) << config.name
              << std::right << std::setw(12) << static_cast<std::uint64_t>(events_per_second)
              << std::setw(12) << percentile(latencies, 0.50) << std::setw(12)
              << percentile(latencies, 0.95) << std::setw(12) << percentile(latencies, 0.99)
              << '\n';
}

} // namespace

int main() {
    constexpr std::size_t total_events = 100'000U;
    constexpr benchmark_case cases[] = {
        {"SPSC", 1U, 1U},
        {"MPSC", 4U, 1U},
        {"SPMC", 1U, 4U},
        {"MPMC", 4U, 4U},
    };

    std::cout << "bounded queue comparison; events=" << total_events << "; capacity=1024\n"
              << std::left << std::setw(12) << "queue" << std::setw(6) << "case" << std::right
              << std::setw(12) << "events/s" << std::setw(12) << "p50 ns" << std::setw(12)
              << "p95 ns" << std::setw(12) << "p99 ns" << '\n';

    for (const auto& config : cases) {
        run_case<bounded_mutex_queue>("mutex", config, total_events);
        run_case<bounded_lock_free_queue>("lock-free", config, total_events);
    }
}
