#include "event_dispatcher/event_dispatcher.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
using event_dispatcher::delivery_guarantee;
using event_dispatcher::delivery_policy;
using event_dispatcher::dispatcher;
using event_dispatcher::dispatcher_config;
using event_dispatcher::subscription_options;
using event_dispatcher::queue::queue_status;

[[nodiscard]] std::int64_t percentile(const std::vector<std::int64_t>& sorted,
                                      std::size_t numerator) {
    const auto index = ((sorted.size() * numerator) + 99U) / 100U - 1U;
    return sorted[index];
}

void run_case(std::string_view name, delivery_policy policy) {
    constexpr std::size_t event_count = 100'000U;
    auto config = dispatcher_config{};
    config.queue_capacity = 1'024U;
    config.worker_count = 4U;
    config.worker_batch_size = 16U;
    dispatcher<std::size_t> instance{config};

    subscription_options options;
    options.delivery = policy;
    options.guarantee = delivery_guarantee::lossless;
    options.mailbox_capacity = 1'024U;
    std::atomic<std::size_t> delivered{0U};
    auto token = instance.subscribe(
        [&](const std::size_t&) { delivered.fetch_add(1U, std::memory_order_relaxed); }, options);

    std::vector<std::int64_t> latencies;
    latencies.reserve(event_count);
    const auto begin = clock_type::now();
    for (std::size_t event = 0U; event < event_count; ++event) {
        const auto publish_begin = clock_type::now();
        if (instance.publish(event) != queue_status::success) {
            throw std::runtime_error{"delivery benchmark publication failed"};
        }
        latencies.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(clock_type::now() - publish_begin)
                .count());
    }
    instance.shutdown();
    const auto end = clock_type::now();
    if (delivered.load(std::memory_order_relaxed) != event_count) {
        throw std::runtime_error{"delivery benchmark accounting failed"};
    }

    std::sort(latencies.begin(), latencies.end());
    const auto events_per_second = static_cast<std::uint64_t>(
        static_cast<double>(event_count) / std::chrono::duration<double>(end - begin).count());
    std::cout << std::left << std::setw(12) << name << std::right << std::setw(14)
              << events_per_second << std::setw(14) << percentile(latencies, 50U) << std::setw(14)
              << percentile(latencies, 95U) << std::setw(14) << percentile(latencies, 99U) << '\n';
    static_cast<void>(token);
}

} // namespace

int main() {
    std::cout << "delivery policy comparison; events=100000; workers=4; worker-batch=16\n"
              << std::left << std::setw(12) << "policy" << std::right << std::setw(14) << "events/s"
              << std::setw(14) << "p50 ns" << std::setw(14) << "p95 ns" << std::setw(14) << "p99 ns"
              << '\n';
    run_case("concurrent", delivery_policy::concurrent);
    run_case("serialized", delivery_policy::serialized);
    run_case("isolated", delivery_policy::isolated);
}
