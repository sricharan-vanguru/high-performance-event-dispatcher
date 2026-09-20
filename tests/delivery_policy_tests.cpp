#include "event_dispatcher/event_dispatcher.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using event_dispatcher::delivery_guarantee;
using event_dispatcher::delivery_policy;
using event_dispatcher::dispatcher;
using event_dispatcher::dispatcher_config;
using event_dispatcher::subscription_options;
using event_dispatcher::queue::queue_status;
using namespace std::chrono_literals;

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

template <typename Predicate> void wait_for_condition(Predicate predicate, const char* message) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error{message};
        }
        std::this_thread::yield();
    }
}

void update_max(std::atomic<int>& maximum, int candidate) {
    auto observed = maximum.load(std::memory_order_relaxed);
    while (observed < candidate &&
           !maximum.compare_exchange_weak(observed, candidate, std::memory_order_relaxed)) {
    }
}

subscription_options
isolated_options(std::size_t capacity,
                 delivery_guarantee guarantee = delivery_guarantee::best_effort) {
    subscription_options options;
    options.delivery = delivery_policy::isolated;
    options.guarantee = guarantee;
    options.mailbox_capacity = capacity;
    return options;
}

void test_option_validation() {
    dispatcher<int> instance;
    auto invalid_capacity = isolated_options(0U);
    bool rejected = false;
    try {
        static_cast<void>(instance.subscribe([](const int&) {}, invalid_capacity));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "zero-capacity isolated mailbox was accepted");

    subscription_options invalid_threshold;
    invalid_threshold.slow_callback_threshold = -1ns;
    rejected = false;
    try {
        static_cast<void>(instance.subscribe([](const int&) {}, invalid_threshold));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "negative slow-callback threshold was accepted");
    instance.shutdown();

    dispatcher<std::unique_ptr<int>> move_only;
    rejected = false;
    try {
        static_cast<void>(
            move_only.subscribe([](const std::unique_ptr<int>&) {}, isolated_options(2U)));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "isolated delivery accepted a move-only Event");
    move_only.shutdown();
}

void test_concurrent_and_serialized_policies() {
    {
        auto config = dispatcher_config{};
        config.worker_count = 2U;
        config.queue_capacity = 8U;
        dispatcher<int> instance{config};
        std::atomic<int> active{0};
        std::atomic<int> maximum{0};
        std::atomic<bool> release{false};
        auto token = instance.subscribe([&](const int&) {
            const auto now = active.fetch_add(1, std::memory_order_relaxed) + 1;
            update_max(maximum, now);
            while (!release.load(std::memory_order_acquire)) {
                release.wait(false, std::memory_order_relaxed);
            }
            active.fetch_sub(1, std::memory_order_relaxed);
        });

        expect(instance.publish(1) == queue_status::success, "concurrent publish failed");
        expect(instance.publish(2) == queue_status::success, "concurrent publish failed");
        wait_for_condition([&] { return active.load(std::memory_order_relaxed) == 2; },
                           "default subscriber did not execute concurrently");
        release.store(true, std::memory_order_release);
        release.notify_all();
        instance.shutdown();
        expect(maximum.load(std::memory_order_relaxed) == 2,
               "concurrent policy unexpectedly serialized callbacks");
        static_cast<void>(token);
    }

    {
        auto config = dispatcher_config{};
        config.worker_count = 4U;
        config.queue_capacity = 32U;
        dispatcher<int> instance{config};
        subscription_options options;
        options.delivery = delivery_policy::serialized;
        std::atomic<int> active{0};
        std::atomic<int> maximum{0};
        auto token = instance.subscribe(
            [&](const int&) {
                const auto now = active.fetch_add(1, std::memory_order_relaxed) + 1;
                update_max(maximum, now);
                std::this_thread::sleep_for(100us);
                active.fetch_sub(1, std::memory_order_relaxed);
            },
            options);
        for (int value = 0; value < 100; ++value) {
            expect(instance.publish(value) == queue_status::success, "serialized publish failed");
        }
        instance.shutdown();
        expect(maximum.load(std::memory_order_relaxed) == 1,
               "serialized subscriber callbacks overlapped");
        expect(token.metrics().delivered == 100U, "serialized delivery count mismatch");
    }
}

void test_isolation_fairness_and_fifo() {
    auto config = dispatcher_config{};
    config.worker_count = 1U;
    config.queue_capacity = 8U;
    dispatcher<int> instance{config};
    std::atomic<bool> release_slow{false};
    std::atomic<bool> slow_started{false};
    std::atomic<std::size_t> fast_calls{0U};
    std::vector<int> slow_order;
    std::mutex slow_order_mutex;

    auto slow = instance.subscribe(
        [&](const int& event) {
            if (event == 1) {
                slow_started.store(true, std::memory_order_release);
                slow_started.notify_all();
                while (!release_slow.load(std::memory_order_acquire)) {
                    release_slow.wait(false, std::memory_order_relaxed);
                }
            }
            std::lock_guard lock{slow_order_mutex};
            slow_order.push_back(event);
        },
        isolated_options(4U));
    auto fast = instance.subscribe(
        [&](const int&) { fast_calls.fetch_add(1U, std::memory_order_relaxed); });

    expect(instance.publish(1) == queue_status::success, "first isolation publish failed");
    wait_for_condition([&] { return slow_started.load(std::memory_order_acquire); },
                       "isolated callback did not start");
    expect(instance.publish(2) == queue_status::success, "second isolation publish failed");
    wait_for_condition([&] { return fast_calls.load(std::memory_order_relaxed) == 2U; },
                       "slow isolated subscriber starved direct subscriber");

    release_slow.store(true, std::memory_order_release);
    release_slow.notify_all();
    instance.shutdown();
    expect((slow_order == std::vector<int>{1, 2}), "isolated mailbox did not preserve FIFO order");
    expect(slow.metrics().delivered == 2U && slow.metrics().dropped == 0U,
           "isolated FIFO metrics mismatch");
    static_cast<void>(fast);
}

void test_best_effort_saturation_metrics() {
    dispatcher<int> instance;
    std::atomic<bool> release{false};
    std::atomic<bool> started{false};
    auto token = instance.subscribe(
        [&](const int& event) {
            if (event == 0) {
                started.store(true, std::memory_order_release);
                while (!release.load(std::memory_order_acquire)) {
                    release.wait(false, std::memory_order_relaxed);
                }
            }
        },
        isolated_options(1U));

    constexpr int event_count = 20;
    for (int event = 0; event < event_count; ++event) {
        expect(instance.publish(event) == queue_status::success, "best-effort publication failed");
        if (event == 0) {
            wait_for_condition([&] { return started.load(std::memory_order_acquire); },
                               "best-effort callback did not start");
        }
    }
    wait_for_condition([&] { return token.metrics().offered == event_count; },
                       "best-effort offers were not observed");
    release.store(true, std::memory_order_release);
    release.notify_all();
    instance.shutdown();

    const auto metrics = token.metrics();
    expect(metrics.offered == event_count, "best-effort offered count mismatch");
    expect(metrics.delivered == 2U, "best-effort delivered count mismatch");
    expect(metrics.dropped == event_count - 2U, "best-effort drop count mismatch");
    expect(metrics.rejected == 0U, "best-effort unexpectedly rejected delivery");
}

void test_lossless_backpressure_and_drain() {
    auto config = dispatcher_config{};
    config.worker_count = 1U;
    config.queue_capacity = 8U;
    dispatcher<int> instance{config};
    std::atomic<bool> release{false};
    std::atomic<bool> started{false};
    std::vector<int> observed;
    auto token = instance.subscribe(
        [&](const int& event) {
            if (event == 0) {
                started.store(true, std::memory_order_release);
                while (!release.load(std::memory_order_acquire)) {
                    release.wait(false, std::memory_order_relaxed);
                }
            }
            observed.push_back(event);
        },
        isolated_options(1U, delivery_guarantee::lossless));

    for (int event = 0; event < 3; ++event) {
        expect(instance.publish(event) == queue_status::success, "lossless publication failed");
        if (event == 0) {
            wait_for_condition([&] { return started.load(std::memory_order_acquire); },
                               "lossless callback did not start");
        }
    }
    release.store(true, std::memory_order_release);
    release.notify_all();
    instance.shutdown();

    expect((observed == std::vector<int>{0, 1, 2}), "lossless mailbox lost or reordered events");
    const auto metrics = token.metrics();
    expect(metrics.delivered == 3U && metrics.dropped == 0U && metrics.rejected == 0U,
           "lossless mailbox metrics mismatch");
}

void test_isolated_unsubscribe_and_discard() {
    {
        dispatcher<int> instance;
        std::atomic<bool> release{false};
        std::atomic<bool> started{false};
        auto token = instance.subscribe(
            [&](const int& event) {
                if (event == 0) {
                    started.store(true, std::memory_order_release);
                    while (!release.load(std::memory_order_acquire)) {
                        release.wait(false, std::memory_order_relaxed);
                    }
                }
            },
            isolated_options(4U));
        expect(instance.publish(0) == queue_status::success, "unsubscribe setup failed");
        wait_for_condition([&] { return started.load(std::memory_order_acquire); },
                           "unsubscribe callback did not start");
        expect(instance.publish(1) == queue_status::success, "unsubscribe queued publish failed");
        wait_for_condition([&] { return token.metrics().offered == 2U; },
                           "unsubscribe queued offer missing");

        std::promise<void> reset_done;
        auto reset_future = reset_done.get_future();
        std::thread resetter{[token = std::move(token), &reset_done]() mutable {
            token.reset();
            reset_done.set_value();
        }};
        expect(reset_future.wait_for(50ms) == std::future_status::timeout,
               "isolated unsubscribe returned during active callback");
        release.store(true, std::memory_order_release);
        release.notify_all();
        expect(reset_future.wait_for(2s) == std::future_status::ready,
               "isolated unsubscribe did not complete");
        resetter.join();
        instance.shutdown();
    }

    {
        auto config = dispatcher_config{};
        config.shutdown = event_dispatcher::shutdown_policy::discard;
        dispatcher<int> instance{config};
        std::atomic<bool> release{false};
        std::atomic<bool> started{false};
        auto token = instance.subscribe(
            [&](const int& event) {
                if (event == 0) {
                    started.store(true, std::memory_order_release);
                    while (!release.load(std::memory_order_acquire)) {
                        release.wait(false, std::memory_order_relaxed);
                    }
                }
            },
            isolated_options(4U));
        expect(instance.publish(0) == queue_status::success, "discard setup failed");
        wait_for_condition([&] { return started.load(std::memory_order_acquire); },
                           "discard callback did not start");
        expect(instance.publish(1) == queue_status::success, "discard mailbox setup failed");
        wait_for_condition([&] { return token.metrics().offered == 2U; },
                           "discard mailbox offer missing");

        std::thread stopper{[&] { instance.shutdown(); }};
        wait_for_condition([&] { return token.metrics().dropped == 1U; },
                           "discard did not clear isolated mailbox");
        release.store(true, std::memory_order_release);
        release.notify_all();
        stopper.join();
        expect(token.metrics().delivered == 1U, "discard interrupted active isolated callback");
    }
}

void test_errors_slow_callbacks_and_reentrancy() {
    {
        std::atomic<std::size_t> errors{0U};
        dispatcher<int> instance{
            {}, [&](std::exception_ptr) { errors.fetch_add(1U, std::memory_order_relaxed); }};
        auto options = isolated_options(2U);
        options.slow_callback_threshold = 1ns;
        auto token = instance.subscribe(
            [](const int&) { throw std::runtime_error{"isolated callback"}; }, options);
        expect(instance.publish(1) == queue_status::success, "isolated error publish failed");
        instance.shutdown();
        const auto metrics = token.metrics();
        expect(errors.load(std::memory_order_relaxed) == 1U && metrics.callback_errors == 1U,
               "isolated callback error was not reported");
        expect(metrics.delivered == 1U && metrics.slow_callbacks == 1U,
               "slow isolated callback metrics mismatch");
    }

    {
        dispatcher<int> instance;
        std::atomic<std::size_t> calls{0U};
        auto token = instance.subscribe(
            [&](const int& event) {
                calls.fetch_add(1U, std::memory_order_relaxed);
                if (event == 1) {
                    expect(instance.try_publish(2) == queue_status::success,
                           "isolated reentrant publication failed");
                }
            },
            isolated_options(4U));
        expect(instance.publish(1) == queue_status::success, "reentrant setup failed");
        wait_for_condition([&] { return calls.load(std::memory_order_relaxed) == 2U; },
                           "isolated reentrant event was not delivered");
        instance.shutdown();
        static_cast<void>(token);
    }

    {
        dispatcher<int> instance;
        std::atomic<bool> rejected{false};
        auto token = instance.subscribe(
            [&](const int&) {
                try {
                    instance.shutdown();
                } catch (const std::logic_error&) {
                    rejected.store(true, std::memory_order_relaxed);
                }
            },
            isolated_options(2U));
        expect(instance.publish(1) == queue_status::success, "self-shutdown setup failed");
        wait_for_condition([&] { return rejected.load(std::memory_order_relaxed); },
                           "isolated callback self-shutdown was not rejected");
        instance.shutdown();
        static_cast<void>(token);
    }
}

void test_isolated_self_unsubscribe() {
    dispatcher<int> instance;
    event_dispatcher::subscription token;
    std::atomic<bool> completed{false};
    token = instance.subscribe(
        [&](const int&) {
            token.reset();
            completed.store(true, std::memory_order_release);
        },
        isolated_options(2U));
    expect(instance.publish(1) == queue_status::success, "self-unsubscribe publish failed");
    wait_for_condition([&] { return completed.load(std::memory_order_acquire); },
                       "isolated self-unsubscribe deadlocked");
    expect(!token, "isolated self-unsubscribe retained token");
    instance.shutdown();
}

} // namespace

int main() {
    test_option_validation();
    test_concurrent_and_serialized_policies();
    test_isolation_fairness_and_fifo();
    test_best_effort_saturation_metrics();
    test_lossless_backpressure_and_drain();
    test_isolated_unsubscribe_and_discard();
    test_errors_slow_callbacks_and_reentrancy();
    test_isolated_self_unsubscribe();
}
