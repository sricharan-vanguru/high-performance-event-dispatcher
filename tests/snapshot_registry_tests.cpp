#include "event_dispatcher/registry/snapshot_registry.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace {

using event_dispatcher::registry::snapshot_registry;
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

template <typename Snapshot>
std::size_t invoke_all(const Snapshot& snapshot, int event) {
    std::size_t invoked = 0U;
    for (const auto& subscriber : *snapshot) {
        if (subscriber->try_invoke(event)) {
            ++invoked;
        }
    }
    return invoked;
}

void test_subscribe_snapshot_and_unsubscribe() {
    snapshot_registry<int> registry;
    expect(registry.acquire_snapshot()->empty(), "new registry must be empty");

    bool rejected_empty = false;
    try {
        static_cast<void>(registry.subscribe({}));
    } catch (const std::invalid_argument&) {
        rejected_empty = true;
    }
    expect(rejected_empty, "empty callback must be rejected");

    int observed = 0;
    auto token = registry.subscribe([&observed](const int& event) { observed = event; });
    expect(token.subscribed(), "new token must be subscribed");
    expect(registry.active_count() == 1U, "active count mismatch after subscribe");

    const auto old_snapshot = registry.acquire_snapshot();
    expect(invoke_all(old_snapshot, 42) == 1U, "active callback was not invoked");
    expect(observed == 42, "callback received wrong event");

    token.reset();
    expect(!token, "reset token must be empty");
    expect(registry.active_count() == 0U, "active count mismatch after unsubscribe");
    expect(registry.acquire_snapshot()->empty(), "new snapshot must remove subscriber");
    expect(invoke_all(old_snapshot, 99) == 0U,
           "old snapshot must not invoke an inactive subscriber");
    expect(observed == 42, "inactive callback changed observed value");
}

void test_token_destruction_unsubscribes() {
    snapshot_registry<int> registry;
    auto snapshot = registry.acquire_snapshot();

    {
        auto token = registry.subscribe([](const int&) {});
        snapshot = registry.acquire_snapshot();
        expect(registry.active_count() == 1U, "active count mismatch before token destruction");
    }

    expect(registry.active_count() == 0U, "token destruction must unsubscribe");
    expect(invoke_all(snapshot, 1) == 0U, "destroyed token callback must remain inactive");
}

void test_token_move_assignment_retires_previous_subscription() {
    snapshot_registry<int> registry;
    std::size_t first_count = 0U;
    std::size_t second_count = 0U;
    auto first = registry.subscribe([&](const int&) { ++first_count; });
    auto second = registry.subscribe([&](const int&) { ++second_count; });
    const auto old_snapshot = registry.acquire_snapshot();

    first = std::move(second);

    expect(first.subscribed(), "move-assigned token lost incoming subscription");
    expect(!second, "moved-from token must be empty");
    expect(registry.active_count() == 1U,
           "move assignment failed to retire previous subscription");
    expect(invoke_all(old_snapshot, 1) == 1U,
           "old snapshot invoked wrong number after token move assignment");
    expect(first_count == 0U, "replaced subscription remained active");
    expect(second_count == 1U, "incoming subscription was not active");
}

void test_synchronous_unsubscribe_waits_for_callback() {
    snapshot_registry<int> registry;
    std::promise<void> callback_entered;
    std::promise<void> release_callback;
    auto callback_entered_future = callback_entered.get_future();
    auto release_future = release_callback.get_future().share();

    auto token = registry.subscribe([&](const int&) {
        callback_entered.set_value();
        release_future.wait();
    });
    const auto snapshot = registry.acquire_snapshot();

    std::thread worker{[snapshot] {
        expect((*snapshot)[0]->try_invoke(1), "callback entry unexpectedly rejected");
    }};
    get_ready(callback_entered_future);

    std::promise<void> unsubscribe_complete;
    auto unsubscribe_future = unsubscribe_complete.get_future();
    std::thread unsubscriber{
        [token = std::move(token), &unsubscribe_complete]() mutable {
            token.reset();
            unsubscribe_complete.set_value();
        }};

    expect(unsubscribe_future.wait_for(50ms) == std::future_status::timeout,
           "unsubscribe returned while callback was still in flight");
    release_callback.set_value();
    worker.join();
    get_ready(unsubscribe_future);
    unsubscriber.join();

    expect((*snapshot)[0]->in_flight() == 0U, "in-flight count did not return to zero");
    expect(!(*snapshot)[0]->subscribed(), "subscriber remained active after unsubscribe");
}

void test_self_unsubscribe_is_deferred_without_deadlock() {
    snapshot_registry<int> registry;
    event_dispatcher::subscription token;
    std::size_t callback_count = 0U;

    token = registry.subscribe([&](const int&) {
        ++callback_count;
        token.reset();
    });
    const auto snapshot = registry.acquire_snapshot();

    expect(invoke_all(snapshot, 1) == 1U, "self-unsubscribing callback did not run");
    expect(callback_count == 1U, "self-unsubscribing callback count mismatch");
    expect(!token, "self-unsubscribe did not clear token");
    expect(registry.active_count() == 0U, "self-unsubscribe did not update registry");
    expect(invoke_all(snapshot, 2) == 0U, "self-unsubscribed callback ran again");
    expect((*snapshot)[0]->in_flight() == 0U, "self-unsubscribe retirement did not complete");
}

void test_callback_exception_releases_in_flight_state() {
    snapshot_registry<int> registry;
    auto token = registry.subscribe([](const int&) { throw std::runtime_error{"callback"}; });
    const auto snapshot = registry.acquire_snapshot();

    bool callback_threw = false;
    try {
        static_cast<void>((*snapshot)[0]->try_invoke(1));
    } catch (const std::runtime_error&) {
        callback_threw = true;
    }
    expect(callback_threw, "callback exception must propagate to future error policy");
    expect((*snapshot)[0]->in_flight() == 0U, "exception leaked in-flight permission");

    token.reset();
    expect(registry.active_count() == 0U, "unsubscribe failed after callback exception");
}

void test_concurrent_unsubscribe_is_idempotent() {
    snapshot_registry<int> registry;
    auto token = registry.subscribe([](const int&) {});
    const auto snapshot = registry.acquire_snapshot();
    const auto state = (*snapshot)[0];

    std::vector<std::thread> threads;
    for (std::size_t index = 0; index < 8U; ++index) {
        threads.emplace_back([state] { state->unsubscribe(); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    expect(!state->subscribed(), "concurrent unsubscribe left state active");
    expect(registry.active_count() == 0U, "concurrent unsubscribe left registry entry active");
    token.reset();
}

void test_snapshot_and_token_outlive_registry_facade() {
    using snapshot_type = snapshot_registry<int>::snapshot;
    snapshot_type snapshot;
    event_dispatcher::subscription token;
    std::atomic<std::size_t> callback_count{0U};

    {
        snapshot_registry<int> registry;
        token = registry.subscribe([&](const int&) {
            callback_count.fetch_add(1U, std::memory_order_relaxed);
        });
        snapshot = registry.acquire_snapshot();
    }

    expect(invoke_all(snapshot, 1) == 1U, "snapshot became invalid with registry facade");
    token.reset();
    expect(invoke_all(snapshot, 1) == 0U, "token failed after registry facade destruction");
    expect(callback_count.load(std::memory_order_relaxed) == 1U,
           "registry lifetime callback count mismatch");
}

void test_subscribe_unsubscribe_during_snapshot_dispatch() {
    snapshot_registry<int> registry;
    std::atomic<bool> stop{false};
    std::atomic<std::size_t> callback_count{0U};
    std::atomic<std::size_t> post_unsubscribe_calls{0U};

    std::thread dispatcher{[&] {
        while (!stop.load(std::memory_order_relaxed)) {
            const auto snapshot = registry.acquire_snapshot();
            static_cast<void>(invoke_all(snapshot, 1));
        }
    }};

    for (std::size_t iteration = 0; iteration < 500U; ++iteration) {
        auto retired = std::make_shared<std::atomic<bool>>(false);
        auto token = registry.subscribe([retired, &callback_count, &post_unsubscribe_calls](
                                            const int&) {
            if (retired->load(std::memory_order_acquire)) {
                post_unsubscribe_calls.fetch_add(1U, std::memory_order_relaxed);
            }
            callback_count.fetch_add(1U, std::memory_order_relaxed);
        });

        std::this_thread::yield();
        token.reset();
        retired->store(true, std::memory_order_release);

        // Dispatch may still own an old immutable snapshot, but logical
        // deactivation must make callback entry fail after reset returns.
        for (std::size_t spin = 0; spin < 10U; ++spin) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_relaxed);
    dispatcher.join();

    expect(post_unsubscribe_calls.load(std::memory_order_relaxed) == 0U,
           "callback began after synchronous unsubscribe returned");
    expect(registry.active_count() == 0U, "stress test left active subscribers");
    static_cast<void>(callback_count); // Count is workload evidence; zero is also legal.
}

} // namespace

int main() {
    test_subscribe_snapshot_and_unsubscribe();
    test_token_destruction_unsubscribes();
    test_token_move_assignment_retires_previous_subscription();
    test_synchronous_unsubscribe_waits_for_callback();
    test_self_unsubscribe_is_deferred_without_deadlock();
    test_callback_exception_releases_in_flight_state();
    test_concurrent_unsubscribe_is_idempotent();
    test_snapshot_and_token_outlive_registry_facade();
    test_subscribe_unsubscribe_during_snapshot_dispatch();
}
