#include "event_dispatcher/dispatcher.hpp"

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace {

using event_dispatcher::dispatcher;
using event_dispatcher::dispatcher_config;
using event_dispatcher::queue::queue_status;
using namespace std::chrono_literals;

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

void test_configuration_validation() {
    auto zero_workers = dispatcher_config{};
    zero_workers.worker_count = 0U;
    bool rejected = false;
    try {
        dispatcher<int> instance{zero_workers};
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "zero workers were accepted");

    auto discard = dispatcher_config{};
    discard.shutdown = event_dispatcher::shutdown_policy::discard;
    rejected = false;
    try {
        dispatcher<int> instance{discard};
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "unimplemented discard policy was accepted");
}

void test_one_event_broadcasts_to_all_subscribers() {
    std::atomic<std::size_t> calls{0U};
    dispatcher<int> instance;
    auto first = instance.subscribe([&](const int& value) {
        expect(value == 42, "first callback received wrong value");
        calls.fetch_add(1U, std::memory_order_relaxed);
    });
    auto second = instance.subscribe([&](const int& value) {
        expect(value == 42, "second callback received wrong value");
        calls.fetch_add(1U, std::memory_order_relaxed);
    });

    expect(instance.publish(42) == queue_status::success, "publish failed");
    instance.shutdown();
    expect(calls.load(std::memory_order_relaxed) == 2U, "broadcast callback count mismatch");
    static_cast<void>(first);
    static_cast<void>(second);
}

void test_multiple_producers_exact_accounting_and_drain() {
    constexpr std::size_t producer_count = 4U;
    constexpr std::size_t events_per_producer = 500U;
    constexpr std::size_t subscriber_count = 3U;

    std::atomic<std::size_t> calls{0U};
    std::vector<event_dispatcher::subscription> subscriptions;
    {
        auto config = dispatcher_config{};
        config.queue_capacity = 32U;
        config.worker_count = 4U;
        dispatcher<std::size_t> instance{config};
        for (std::size_t index = 0U; index < subscriber_count; ++index) {
            subscriptions.push_back(instance.subscribe([&](const std::size_t&) {
                calls.fetch_add(1U, std::memory_order_relaxed);
            }));
        }

        std::vector<std::thread> producers;
        for (std::size_t producer = 0U; producer < producer_count; ++producer) {
            producers.emplace_back([&] {
                for (std::size_t event = 0U; event < events_per_producer; ++event) {
                    expect(instance.publish(event) == queue_status::success,
                           "blocking publication failed");
                }
            });
        }
        for (auto& producer : producers) {
            producer.join();
        }
        // Scope destruction is itself the drain-and-join operation under test.
    }

    expect(calls.load(std::memory_order_relaxed) ==
               producer_count * events_per_producer * subscriber_count,
           "multi-producer callback accounting mismatch");
}

void test_throwing_callbacks_and_error_handler_are_contained() {
    std::atomic<std::size_t> errors{0U};
    std::atomic<std::size_t> healthy_calls{0U};
    dispatcher<int> instance{
        {}, [&](std::exception_ptr error) {
            errors.fetch_add(1U, std::memory_order_relaxed);
            expect(error != nullptr, "missing callback exception");
            throw std::runtime_error{"error handler failure"};
        }};
    auto throwing = instance.subscribe([](const int&) { throw std::runtime_error{"callback"}; });
    auto healthy = instance.subscribe([&](const int&) {
        healthy_calls.fetch_add(1U, std::memory_order_relaxed);
    });

    expect(instance.publish(1) == queue_status::success, "first publish failed");
    expect(instance.publish(2) == queue_status::success, "second publish failed");
    instance.shutdown();
    expect(errors.load(std::memory_order_relaxed) == 2U, "callback errors were not reported");
    expect(healthy_calls.load(std::memory_order_relaxed) == 2U,
           "exception prevented later delivery");
    static_cast<void>(throwing);
    static_cast<void>(healthy);
}

void test_move_only_events_and_no_subscribers() {
    auto config = dispatcher_config{};
    config.queue_capacity = 2U;
    dispatcher<std::unique_ptr<int>> instance{config};
    expect(instance.publish(std::make_unique<int>(7)) == queue_status::success,
           "move-only event publication failed");
    instance.shutdown();
    expect(instance.try_publish(std::make_unique<int>(8)) == queue_status::closed,
           "publication after shutdown was not rejected");
}

void test_non_blocking_publication_reports_full() {
    auto config = dispatcher_config{};
    config.queue_capacity = 1U;
    dispatcher<int> instance{config};
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    auto token = instance.subscribe([&](const int& event) {
        if (event == 1) {
            entered.set_value();
            release_future.wait();
        }
    });

    expect(instance.publish(1) == queue_status::success, "blocking setup publish failed");
    entered_future.get();
    expect(instance.try_publish(2) == queue_status::success, "queue slot was not available");
    expect(instance.try_publish(3) == queue_status::full, "full queue was not reported");
    release.set_value();
    instance.shutdown();
    static_cast<void>(token);
}

void test_shutdown_from_callback_is_rejected_without_deadlock() {
    std::atomic<bool> rejected{false};
    dispatcher<int> instance;
    auto token = instance.subscribe([&](const int&) {
        try {
            instance.shutdown();
        } catch (const std::logic_error&) {
            rejected.store(true, std::memory_order_relaxed);
        }
    });

    expect(instance.publish(1) == queue_status::success, "self-shutdown publish failed");
    instance.shutdown();
    expect(rejected.load(std::memory_order_relaxed), "worker self-shutdown was not rejected");
    static_cast<void>(token);
}

void test_workers_can_deliver_callbacks_concurrently() {
    auto config = dispatcher_config{};
    config.worker_count = 2U;
    dispatcher<int> instance{config};
    std::barrier rendezvous{2};
    std::atomic<std::size_t> calls{0U};
    auto token = instance.subscribe([&](const int&) {
        calls.fetch_add(1U, std::memory_order_relaxed);
        rendezvous.arrive_and_wait();
    });

    expect(instance.publish(1) == queue_status::success, "first concurrent publish failed");
    expect(instance.publish(2) == queue_status::success, "second concurrent publish failed");
    instance.shutdown();
    expect(calls.load(std::memory_order_relaxed) == 2U, "workers did not complete callbacks");
    static_cast<void>(token);
}

void test_one_worker_preserves_dequeue_order() {
    auto config = dispatcher_config{};
    config.worker_count = 1U;
    dispatcher<std::size_t> instance{config};
    std::vector<std::size_t> observed;
    auto token = instance.subscribe([&](const std::size_t& event) {
        observed.push_back(event);
    });

    for (std::size_t event = 0U; event < 100U; ++event) {
        expect(instance.publish(event) == queue_status::success, "ordered publish failed");
    }
    instance.shutdown();
    expect(observed.size() == 100U, "ordered delivery count mismatch");
    for (std::size_t event = 0U; event < observed.size(); ++event) {
        expect(observed[event] == event, "one-worker delivery reordered events");
    }
    static_cast<void>(token);
}

void test_unsubscribe_during_dispatch_waits_and_prevents_reentry() {
    dispatcher<int> instance;
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::atomic<std::size_t> calls{0U};
    auto token = instance.subscribe([&](const int&) {
        calls.fetch_add(1U, std::memory_order_relaxed);
        entered.set_value();
        release_future.wait();
    });

    expect(instance.publish(1) == queue_status::success, "unsubscribe test publish failed");
    entered_future.get();
    std::promise<void> reset_done;
    auto reset_done_future = reset_done.get_future();
    std::thread unsubscriber{[&] {
        token.reset();
        reset_done.set_value();
    }};
    expect(reset_done_future.wait_for(20ms) == std::future_status::timeout,
           "unsubscribe returned during an active callback");
    release.set_value();
    reset_done_future.get();
    unsubscriber.join();

    expect(instance.publish(2) == queue_status::success, "post-unsubscribe publish failed");
    instance.shutdown();
    expect(calls.load(std::memory_order_relaxed) == 1U,
           "callback re-entered after unsubscribe returned");
}

void test_subscription_changes_during_dispatch() {
    auto config = dispatcher_config{};
    config.worker_count = 1U;
    dispatcher<int> instance{config};
    event_dispatcher::subscription added;
    std::promise<void> first_event_complete;
    auto first_event_future = first_event_complete.get_future();
    std::atomic<std::size_t> original_calls{0U};
    std::atomic<std::size_t> added_calls{0U};

    auto original = instance.subscribe([&](const int& event) {
        original_calls.fetch_add(1U, std::memory_order_relaxed);
        if (event == 1) {
            added = instance.subscribe([&](const int&) {
                added_calls.fetch_add(1U, std::memory_order_relaxed);
            });
            first_event_complete.set_value();
        }
    });

    expect(instance.publish(1) == queue_status::success, "first changing publish failed");
    first_event_future.get();
    expect(instance.publish(2) == queue_status::success, "second changing publish failed");
    instance.shutdown();
    expect(original_calls.load(std::memory_order_relaxed) == 2U,
           "original subscriber count mismatch");
    expect(added_calls.load(std::memory_order_relaxed) == 1U,
           "new subscriber observed wrong snapshot");
    static_cast<void>(original);
}

void test_repeated_construction_and_destruction() {
    std::atomic<std::size_t> calls{0U};
    for (std::size_t iteration = 0U; iteration < 50U; ++iteration) {
        dispatcher<int> instance;
        auto token = instance.subscribe([&](const int&) {
            calls.fetch_add(1U, std::memory_order_relaxed);
        });
        expect(instance.publish(1) == queue_status::success, "repeated publish failed");
        instance.shutdown();
        static_cast<void>(token);
    }
    expect(calls.load(std::memory_order_relaxed) == 50U,
           "repeated dispatcher destruction lost events");
}

} // namespace

int main() {
    test_configuration_validation();
    test_one_event_broadcasts_to_all_subscribers();
    test_multiple_producers_exact_accounting_and_drain();
    test_throwing_callbacks_and_error_handler_are_contained();
    test_move_only_events_and_no_subscribers();
    test_non_blocking_publication_reports_full();
    test_shutdown_from_callback_is_rejected_without_deadlock();
    test_workers_can_deliver_callbacks_concurrently();
    test_one_worker_preserves_dequeue_order();
    test_unsubscribe_during_dispatch_waits_and_prevents_reentry();
    test_subscription_changes_during_dispatch();
    test_repeated_construction_and_destruction();
}
