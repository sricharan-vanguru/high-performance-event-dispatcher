#include "event_dispatcher/registry/snapshot_registry.hpp"

#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
using event_dispatcher::registry::snapshot_registry;

[[nodiscard]] double nanoseconds_per_operation(clock_type::duration elapsed,
                                                std::size_t operations) {
    const auto nanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    return static_cast<double>(nanoseconds) / static_cast<double>(operations);
}

void run_case(std::size_t subscriber_count) {
    snapshot_registry<int> registry;
    std::vector<event_dispatcher::subscription> subscriptions;
    subscriptions.reserve(subscriber_count);

    for (std::size_t index = 0; index < subscriber_count; ++index) {
        subscriptions.push_back(registry.subscribe([](const int&) {}));
    }

    constexpr std::size_t read_iterations = 1'000'000U;
    std::size_t observed_entries = 0U;
    const auto read_begin = clock_type::now();
    for (std::size_t iteration = 0; iteration < read_iterations; ++iteration) {
        observed_entries += registry.acquire_snapshot()->size();
    }
    const auto read_end = clock_type::now();

    constexpr std::size_t write_iterations = 1'000U;
    const auto write_begin = clock_type::now();
    for (std::size_t iteration = 0; iteration < write_iterations; ++iteration) {
        auto temporary = registry.subscribe([](const int&) {});
        temporary.reset();
    }
    const auto write_end = clock_type::now();

    // One loop performs two copy-on-write changes: add then remove.
    constexpr std::size_t changes_per_iteration = 2U;
    std::cout << std::setw(12) << subscriber_count << std::setw(18) << std::fixed
              << std::setprecision(2)
              << nanoseconds_per_operation(read_end - read_begin, read_iterations)
              << std::setw(22)
              << nanoseconds_per_operation(
                     write_end - write_begin, write_iterations * changes_per_iteration)
              << std::setw(18) << observed_entries << '\n';
}

} // namespace

int main() {
    constexpr std::size_t subscriber_counts[] = {1U, 10U, 100U, 1'000U};

    std::cout << "snapshot_registry baseline\n"
              << std::setw(12) << "subscribers" << std::setw(18) << "snapshot ns/op"
              << std::setw(22) << "membership ns/change" << std::setw(18)
              << "observed entries" << '\n';

    for (const auto count : subscriber_counts) {
        run_case(count);
    }
}
