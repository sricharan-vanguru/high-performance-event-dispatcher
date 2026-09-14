#include "event_dispatcher/dispatcher.hpp"
#include "event_dispatcher/queue/bounded_lock_free_queue.hpp"
#include "event_dispatcher/registry/snapshot_registry.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <new>
#include <stdexcept>
#include <vector>

namespace {

std::atomic<std::size_t> allocation_count{0U};

using clock_type = std::chrono::steady_clock;
using event_dispatcher::dispatcher;
using event_dispatcher::dispatcher_config;
using event_dispatcher::queue::bounded_lock_free_queue;
using event_dispatcher::queue::queue_status;

struct measured_event final {
    explicit measured_event(std::size_t initial_value) noexcept : value(initial_value) {}
    measured_event(measured_event&&) noexcept = default;
    std::size_t value;
};

struct large_capture final {
    std::array<std::byte, 1'024U> bytes{};
};

void callback_target(const std::size_t& value) noexcept {
    static std::atomic<std::size_t> sink{0U};
    sink.fetch_add(value, std::memory_order_relaxed);
}

[[nodiscard]] std::size_t allocations_since(std::size_t before) noexcept {
    return allocation_count.load(std::memory_order_relaxed) - before;
}

[[nodiscard]] std::int64_t percentile(const std::vector<std::int64_t>& sorted,
                                      std::size_t numerator) {
    const auto index = ((sorted.size() * numerator) + 99U) / 100U - 1U;
    return sorted[index];
}

void measure_queue_allocations() {
    bounded_lock_free_queue<measured_event> queue{1'024U};
    constexpr std::size_t operations = 100'000U;
    const auto before = allocation_count.load(std::memory_order_relaxed);
    for (std::size_t index = 0U; index < operations; ++index) {
        if (queue.try_emplace(index) != queue_status::success || !queue.try_pop()) {
            throw std::runtime_error{"allocation benchmark queue operation failed"};
        }
    }
    std::cout << "successful lock-free try_emplace+try_pop allocations: "
              << allocations_since(before) << " for " << operations << " pairs\n";
}

void measure_subscription_allocations() {
    event_dispatcher::registry::snapshot_registry<std::size_t> registry;

    auto before = allocation_count.load(std::memory_order_relaxed);
    auto small = registry.subscribe(callback_target);
    const auto small_allocations = allocations_since(before);

    large_capture capture;
    before = allocation_count.load(std::memory_order_relaxed);
    auto large = registry.subscribe([capture](const std::size_t& value) {
        if (capture.bytes[0] == std::byte{1}) {
            callback_target(value);
        }
    });
    const auto large_allocations = allocations_since(before);

    std::cout << "subscription allocations (includes state and snapshot): small="
              << small_allocations << ", 1 KiB capture=" << large_allocations << '\n';
    static_cast<void>(small);
    static_cast<void>(large);
}

void measure_callback_calls() {
    constexpr std::size_t calls = 2'000'000U;
    std::function<void(const std::size_t&)> erased = callback_target;
    const std::size_t value = 1U;

    const auto direct_begin = clock_type::now();
    for (std::size_t index = 0U; index < calls; ++index) {
        callback_target(value);
    }
    const auto direct_end = clock_type::now();

    const auto erased_begin = clock_type::now();
    for (std::size_t index = 0U; index < calls; ++index) {
        erased(value);
    }
    const auto erased_end = clock_type::now();

    const auto ns_per_call = [](clock_type::duration elapsed) {
        return static_cast<double>(
                   std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()) /
               static_cast<double>(calls);
    };
    std::cout << std::fixed << std::setprecision(2)
              << "callback ns/call: direct=" << ns_per_call(direct_end - direct_begin)
              << ", std::function=" << ns_per_call(erased_end - erased_begin) << '\n';
}

void measure_worker_batch(std::size_t batch_size) {
    constexpr std::size_t events = 200'000U;
    auto config = dispatcher_config{};
    config.queue_capacity = 1'024U;
    config.worker_count = 1U;
    config.worker_batch_size = batch_size;
    dispatcher<std::size_t, bounded_lock_free_queue> instance{config};
    std::size_t delivered = 0U;
    std::vector<std::int64_t> latencies;
    latencies.reserve(events);
    auto token = instance.subscribe([&](const std::size_t&) { ++delivered; });

    const auto begin = clock_type::now();
    for (std::size_t value = 0U; value < events; ++value) {
        const auto publish_begin = clock_type::now();
        if (instance.publish(value) != queue_status::success) {
            throw std::runtime_error{"batch benchmark publication failed"};
        }
        latencies.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(clock_type::now() - publish_begin)
                .count());
    }
    instance.shutdown();
    const auto end = clock_type::now();
    if (delivered != events) {
        throw std::runtime_error{"batch benchmark delivery mismatch"};
    }

    const double seconds = std::chrono::duration<double>(end - begin).count();
    std::sort(latencies.begin(), latencies.end());
    std::cout << "worker batch=" << std::setw(2) << batch_size << ": "
              << static_cast<std::size_t>(static_cast<double>(events) / seconds)
              << " events/s, publish p50/p95/p99=" << percentile(latencies, 50U) << '/'
              << percentile(latencies, 95U) << '/' << percentile(latencies, 99U) << " ns\n";
    static_cast<void>(token);
}

} // namespace

void* operator new(std::size_t size) {
    allocation_count.fetch_add(1U, std::memory_order_relaxed);
    if (void* memory = std::malloc(size)) {
        return memory;
    }
    throw std::bad_alloc{};
}

void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

int main() {
    std::cout << "allocation and batching measurements\n";
    measure_queue_allocations();
    measure_subscription_allocations();
    measure_callback_calls();
    for (const auto batch_size : {1U, 4U, 16U, 64U}) {
        measure_worker_batch(batch_size);
    }
}
