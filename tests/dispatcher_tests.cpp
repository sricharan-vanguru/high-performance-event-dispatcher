#include "event_dispatcher/dispatcher.hpp"

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

template <typename Predicate> void wait_for_condition(Predicate predicate, const char* message) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error{message};
        }
        std::this_thread::yield();
    }
}

void test_configuration_validation() {
    auto zero_capacity = dispatcher_config{};
    zero_capacity.queue_capacity = 0U;
    bool rejected = false;
    try {
        dispatcher<int> instance{zero_capacity};
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "zero queue capacity was accepted");

    auto zero_workers = dispatcher_config{};
    zero_workers.worker_count = 0U;
    rejected = false;
    try {
        dispatcher<int> instance{zero_workers};
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "zero workers were accepted");

    auto discard = dispatcher_config{};
    discard.shutdown = event_dispatcher::shutdown_policy::discard;
    dispatcher<int> discard_instance{discard};
    discard_instance.shutdown();
    expect(discard_instance.lifecycle() == event_dispatcher::lifecycle_state::stopped,
           "configured discard shutdown did not stop");
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
            subscriptions.push_back(instance.subscribe(
                [&](const std::size_t&) { calls.fetch_add(1U, std::memory_order_relaxed); }));
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
    dispatcher<int> instance{{}, [&](std::exception_ptr error) {
                                 errors.fetch_add(1U, std::memory_order_relaxed);
                                 expect(error != nullptr, "missing callback exception");
                                 throw std::runtime_error{"error handler failure"};
                             }};
    auto throwing = instance.subscribe([](const int&) { throw std::runtime_error{"callback"}; });
    auto healthy = instance.subscribe(
        [&](const int&) { healthy_calls.fetch_add(1U, std::memory_order_relaxed); });

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
    const auto counters = instance.metrics();
    expect(counters.accepted == 1U && counters.rejected == 1U,
           "post-shutdown rejection metrics mismatch");
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
    const auto counters = instance.metrics();
    expect(counters.accepted == 2U && counters.rejected == 1U,
           "full-queue rejection metrics mismatch");
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
    auto token = instance.subscribe([&](const std::size_t& event) { observed.push_back(event); });

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
            added = instance.subscribe(
                [&](const int&) { added_calls.fetch_add(1U, std::memory_order_relaxed); });
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

void test_reentrant_non_blocking_publication() {
    auto config = dispatcher_config{};
    config.worker_count = 1U;
    config.queue_capacity = 2U;
    dispatcher<int> instance{config};
    std::atomic<std::size_t> calls{0U};
    std::atomic<queue_status> nested_result{queue_status::empty};
    std::promise<void> nested_complete;
    auto nested_complete_future = nested_complete.get_future();
    auto token = instance.subscribe([&](const int& event) {
        calls.fetch_add(1U, std::memory_order_relaxed);
        if (event == 1) {
            nested_result.store(instance.try_publish(2), std::memory_order_relaxed);
            nested_complete.set_value();
        }
    });

    expect(instance.publish(1) == queue_status::success, "reentrant setup publish failed");
    nested_complete_future.get();
    instance.shutdown();
    expect(nested_result.load(std::memory_order_relaxed) == queue_status::success,
           "reentrant non-blocking publication failed");
    expect(calls.load(std::memory_order_relaxed) == 2U, "reentrant publication was not delivered");
    static_cast<void>(token);
}

void test_repeated_construction_and_destruction() {
    std::atomic<std::size_t> calls{0U};
    for (std::size_t iteration = 0U; iteration < 50U; ++iteration) {
        dispatcher<int> instance;
        auto token =
            instance.subscribe([&](const int&) { calls.fetch_add(1U, std::memory_order_relaxed); });
        expect(instance.publish(1) == queue_status::success, "repeated publish failed");
        instance.shutdown();
        static_cast<void>(token);
    }
    expect(calls.load(std::memory_order_relaxed) == 50U,
           "repeated dispatcher destruction lost events");
}

void test_drain_transition_rejection_and_metrics() {
    auto config = dispatcher_config{};
    config.queue_capacity = 4U;
    dispatcher<int> instance{config};
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::atomic<std::size_t> calls{0U};
    auto token = instance.subscribe([&](const int& event) {
        calls.fetch_add(1U, std::memory_order_relaxed);
        if (event == 1) {
            entered.set_value();
            release_future.wait();
        }
    });

    expect(instance.lifecycle() == event_dispatcher::lifecycle_state::running,
           "constructed dispatcher was not running");
    expect(instance.publish(1) == queue_status::success, "drain setup publish failed");
    entered_future.get();
    expect(instance.publish(2) == queue_status::success, "queued drain publish failed");

    std::thread stopper{[&] { instance.shutdown(event_dispatcher::shutdown_policy::drain); }};
    wait_for_condition(
        [&] { return instance.lifecycle() == event_dispatcher::lifecycle_state::drain_stopping; },
        "drain-stopping state was not observable");
    expect(instance.try_publish(3) == queue_status::closed,
           "publication was accepted after drain began");

    bool subscription_rejected = false;
    try {
        static_cast<void>(instance.subscribe([](const int&) {}));
    } catch (const std::logic_error&) {
        subscription_rejected = true;
    }
    expect(subscription_rejected, "subscription was accepted during drain shutdown");

    release.set_value();
    stopper.join();
    instance.shutdown();
    expect(instance.lifecycle() == event_dispatcher::lifecycle_state::stopped,
           "drain did not reach stopped");
    subscription_rejected = false;
    try {
        static_cast<void>(instance.subscribe([](const int&) {}));
    } catch (const std::logic_error&) {
        subscription_rejected = true;
    }
    expect(subscription_rejected, "subscription was accepted after shutdown");
    expect(calls.load(std::memory_order_relaxed) == 2U, "drain lost an accepted event");
    const auto counters = instance.metrics();
    expect(counters.accepted == 2U && counters.rejected == 1U && counters.dropped == 0U,
           "drain metrics mismatch");
    static_cast<void>(token);
}

void test_discard_drops_only_queued_events() {
    auto config = dispatcher_config{};
    config.queue_capacity = 4U;
    config.shutdown = event_dispatcher::shutdown_policy::discard;
    dispatcher<int> instance{config};
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::atomic<std::size_t> calls{0U};
    auto token = instance.subscribe([&](const int& event) {
        calls.fetch_add(1U, std::memory_order_relaxed);
        if (event == 1) {
            entered.set_value();
            release_future.wait();
        }
    });

    expect(instance.publish(1) == queue_status::success, "discard active publish failed");
    entered_future.get();
    expect(instance.publish(2) == queue_status::success, "first discard queue publish failed");
    expect(instance.publish(3) == queue_status::success, "second discard queue publish failed");

    std::thread stopper{[&] { instance.shutdown(); }};
    wait_for_condition(
        [&] {
            return instance.lifecycle() == event_dispatcher::lifecycle_state::discard_stopping &&
                   instance.metrics().dropped == 2U;
        },
        "discard transition did not remove queued events");
    release.set_value();
    stopper.join();

    expect(calls.load(std::memory_order_relaxed) == 1U,
           "discard interrupted an active callback or delivered queued events");
    const auto counters = instance.metrics();
    expect(counters.accepted == 3U && counters.rejected == 0U && counters.dropped == 2U,
           "discard metrics mismatch");
    static_cast<void>(token);
}

void test_timeout_and_cancellation_backpressure() {
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

    expect(instance.publish(1) == queue_status::success, "timeout active publish failed");
    entered_future.get();
    expect(instance.publish(2) == queue_status::success, "timeout queue fill failed");
    expect(instance.publish_for(3, 20ms) == queue_status::timeout, "bounded wait did not time out");
    std::stop_source cancellation;
    cancellation.request_stop();
    expect(instance.publish_for(4, 1s, cancellation.get_token()) == queue_status::stopped,
           "cancelled publication did not report stopped");

    release.set_value();
    instance.shutdown();
    const auto counters = instance.metrics();
    expect(counters.accepted == 2U && counters.rejected == 2U,
           "timeout/cancellation metrics mismatch");
    static_cast<void>(token);
}

void test_discard_wakes_blocked_producer() {
    auto config = dispatcher_config{};
    config.queue_capacity = 1U;
    dispatcher<int> instance{config};
    std::promise<void> callback_entered;
    auto callback_entered_future = callback_entered.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    auto token = instance.subscribe([&](const int& event) {
        if (event == 1) {
            callback_entered.set_value();
            release_future.wait();
        }
    });

    expect(instance.publish(1) == queue_status::success, "blocked producer setup failed");
    callback_entered_future.get();
    expect(instance.publish(2) == queue_status::success, "blocked producer queue fill failed");
    std::promise<queue_status> producer_result;
    auto producer_future = producer_result.get_future();
    std::thread producer{[&] { producer_result.set_value(instance.publish(3)); }};
    expect(producer_future.wait_for(20ms) == std::future_status::timeout,
           "producer did not block on full queue");

    std::thread stopper{[&] { instance.shutdown(event_dispatcher::shutdown_policy::discard); }};
    expect(producer_future.get() == queue_status::closed,
           "shutdown did not wake blocked producer with closed status");
    release.set_value();
    producer.join();
    stopper.join();
    const auto counters = instance.metrics();
    expect(counters.accepted == 2U && counters.rejected == 1U && counters.dropped == 1U,
           "blocked producer shutdown metrics mismatch");
    static_cast<void>(token);
}

void test_concurrent_shutdown_is_idempotent() {
    dispatcher<int> instance;
    std::atomic<std::size_t> calls{0U};
    auto token =
        instance.subscribe([&](const int&) { calls.fetch_add(1U, std::memory_order_relaxed); });
    for (std::size_t event = 0U; event < 100U; ++event) {
        expect(instance.publish(static_cast<int>(event)) == queue_status::success,
               "concurrent shutdown setup publish failed");
    }

    std::vector<std::thread> stoppers;
    for (std::size_t index = 0U; index < 8U; ++index) {
        stoppers.emplace_back([&] { instance.shutdown(); });
    }
    for (auto& stopper : stoppers) {
        stopper.join();
    }
    expect(instance.lifecycle() == event_dispatcher::lifecycle_state::stopped,
           "concurrent shutdown did not stop");
    expect(calls.load(std::memory_order_relaxed) == 100U, "concurrent drain lost accepted events");
    static_cast<void>(token);
}

void test_first_shutdown_policy_wins() {
    auto config = dispatcher_config{};
    config.queue_capacity = 2U;
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
    expect(instance.publish(1) == queue_status::success, "policy winner setup failed");
    entered_future.get();
    expect(instance.publish(2) == queue_status::success, "policy winner queue fill failed");

    std::thread first{[&] { instance.shutdown(event_dispatcher::shutdown_policy::discard); }};
    wait_for_condition(
        [&] { return instance.lifecycle() == event_dispatcher::lifecycle_state::discard_stopping; },
        "first shutdown policy did not begin");
    std::thread second{[&] { instance.shutdown(event_dispatcher::shutdown_policy::drain); }};
    release.set_value();
    first.join();
    second.join();

    expect(instance.metrics().dropped == 1U, "later shutdown policy replaced first policy");
    static_cast<void>(token);
}

void test_shutdown_transition_matrix_under_traffic() {
    constexpr std::size_t rounds = 30U;
    constexpr std::size_t producer_count = 4U;

    for (std::size_t round = 0U; round < rounds; ++round) {
        auto config = dispatcher_config{};
        config.queue_capacity = 8U;
        config.worker_count = 4U;
        dispatcher<std::size_t> instance{config};
        std::atomic<std::uint64_t> callback_count{0U};
        auto token = instance.subscribe(
            [&](const std::size_t&) { callback_count.fetch_add(1U, std::memory_order_relaxed); });
        std::barrier start_line{static_cast<std::ptrdiff_t>(producer_count + 1U)};
        std::vector<std::thread> producers;
        for (std::size_t producer = 0U; producer < producer_count; ++producer) {
            producers.emplace_back([&, producer] {
                start_line.arrive_and_wait();
                std::size_t sequence = producer;
                while (instance.publish(sequence) == queue_status::success) {
                    sequence += producer_count;
                }
            });
        }

        start_line.arrive_and_wait();
        for (std::size_t spin = 0U; spin < 100U; ++spin) {
            std::this_thread::yield();
        }
        const auto policy = round % 2U == 0U ? event_dispatcher::shutdown_policy::drain
                                             : event_dispatcher::shutdown_policy::discard;
        instance.shutdown(policy);
        for (auto& producer : producers) {
            producer.join();
        }

        const auto counters = instance.metrics();
        expect(callback_count.load(std::memory_order_relaxed) + counters.dropped ==
                   counters.accepted,
               "shutdown race lost or duplicated an accepted event");
        expect(counters.rejected == producer_count,
               "shutdown race did not release every producer exactly once");
        if (policy == event_dispatcher::shutdown_policy::drain) {
            expect(counters.dropped == 0U, "drain race unexpectedly dropped events");
        }
        static_cast<void>(token);
    }
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
    test_reentrant_non_blocking_publication();
    test_repeated_construction_and_destruction();
    test_drain_transition_rejection_and_metrics();
    test_discard_drops_only_queued_events();
    test_timeout_and_cancellation_backpressure();
    test_discard_wakes_blocked_producer();
    test_concurrent_shutdown_is_idempotent();
    test_first_shutdown_policy_wins();
    test_shutdown_transition_matrix_under_traffic();
}
