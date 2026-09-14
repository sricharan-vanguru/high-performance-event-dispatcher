#include "event_dispatcher/event_dispatcher.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <latch>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using event_dispatcher::dispatcher;
using event_dispatcher::dispatcher_config;
using event_dispatcher::queue::bounded_lock_free_queue;
using event_dispatcher::queue::bounded_mutex_queue;
using event_dispatcher::queue::queue_status;

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

struct emplaced_event final {
    static inline std::atomic<std::size_t> moves{0U};
    static inline bool throw_during_construction{false};

    emplaced_event(int initial_number, std::string initial_text)
        : number(initial_number), text(std::move(initial_text)) {
        if (throw_during_construction) {
            throw std::runtime_error{"requested construction failure"};
        }
    }

    emplaced_event(emplaced_event&& other) noexcept
        : number(other.number), text(std::move(other.text)) {
        moves.fetch_add(1U, std::memory_order_relaxed);
    }

    emplaced_event(const emplaced_event&) = delete;
    emplaced_event& operator=(const emplaced_event&) = delete;

    int number;
    std::string text;
};

template <template <typename> class Queue> void test_queue_emplacement() {
    Queue<emplaced_event> queue{2U};
    emplaced_event::moves.store(0U, std::memory_order_relaxed);

    expect(queue.try_emplace(7, "direct") == queue_status::success, "direct emplacement failed");
    expect(emplaced_event::moves.load(std::memory_order_relaxed) == 0U,
           "emplacement created an Event temporary");

    expect(queue.try_emplace(8, "second") == queue_status::success, "second emplacement failed");
    std::string retained = "not-consumed";
    expect(queue.try_emplace(9, std::move(retained)) == queue_status::full,
           "full queue accepted emplacement");
    expect(retained == "not-consumed", "failed emplacement consumed an argument");

    auto first = queue.try_pop();
    expect(first && first.value().number == 7 && first.value().text == "direct",
           "emplaced value was corrupted");

    emplaced_event::throw_during_construction = true;
    bool threw = false;
    try {
        static_cast<void>(queue.try_emplace(10, "throws"));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    emplaced_event::throw_during_construction = false;
    expect(threw, "throwing constructor did not propagate");
    expect(queue.size() == 1U, "throwing emplacement changed queue ownership");
    expect(queue.try_emplace(11, "recovered") == queue_status::success,
           "queue did not recover after throwing emplacement");
}

void test_dispatcher_configuration_and_emplacement() {
    auto invalid = dispatcher_config{};
    invalid.worker_batch_size = 0U;
    bool rejected = false;
    try {
        dispatcher<int> instance{invalid};
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    expect(rejected, "zero worker batch size was accepted");

    dispatcher<emplaced_event> instance;
    std::atomic<int> observed{0};
    auto token = instance.subscribe([&](const emplaced_event& event) {
        observed.store(event.number, std::memory_order_relaxed);
    });
    emplaced_event::moves.store(0U, std::memory_order_relaxed);
    expect(instance.try_emplace(42, "dispatcher") == queue_status::success,
           "dispatcher emplacement failed");
    instance.shutdown();
    expect(observed.load(std::memory_order_relaxed) == 42,
           "dispatcher did not deliver emplaced event");
    expect(instance.metrics().accepted == 1U, "emplacement acceptance metric mismatch");
    static_cast<void>(token);
}

void test_empty_copy_and_closed_batches() {
    dispatcher<int> instance;
    const std::array<int, 2> copied{4, 5};
    std::atomic<std::size_t> calls{0U};
    auto token =
        instance.subscribe([&](const int&) { calls.fetch_add(1U, std::memory_order_relaxed); });

    const auto empty = instance.try_publish_batch(std::span<const int>{});
    expect(empty.requested == 0U && empty.accepted == 0U && empty.complete(),
           "empty batch was not a complete no-op");
    const auto complete = instance.try_publish_batch(std::span<const int>{copied});
    expect(complete.requested == 2U && complete.accepted == 2U && complete.complete(),
           "copy batch did not complete");
    instance.shutdown();
    expect(calls.load(std::memory_order_relaxed) == 2U, "copy batch delivery mismatch");

    const auto closed = instance.try_publish_batch(std::span<const int>{copied});
    expect(closed.requested == 2U && closed.accepted == 0U && closed.status == queue_status::closed,
           "closed batch result mismatch");
    expect(instance.metrics().rejected == 1U,
           "unattempted closed-batch suffix was incorrectly counted as rejected");
    static_cast<void>(token);
}

template <template <typename> class Queue>
void test_partial_batch(std::size_t capacity, std::size_t expected_accepted) {
    auto config = dispatcher_config{};
    config.queue_capacity = capacity;
    config.worker_count = 1U;
    config.worker_batch_size = capacity;
    dispatcher<std::string, Queue> instance{config};

    std::latch callback_entered{1};
    std::latch release_callback{1};
    auto token = instance.subscribe([&](const std::string& event) {
        if (event == "block") {
            callback_entered.count_down();
            release_callback.wait();
        }
    });

    expect(instance.publish(std::string{"block"}) == queue_status::success,
           "blocking event publication failed");
    callback_entered.wait();

    std::array<std::string, 3> events{"first", "second", "third"};
    const auto result = instance.try_publish_batch(std::span<std::string>{events});
    expect(result.requested == events.size() && result.accepted == expected_accepted,
           "partial batch accepted count mismatch");
    expect(result.status == queue_status::full && !result.complete(),
           "partial batch terminal status mismatch");
    for (std::size_t index = expected_accepted; index < events.size(); ++index) {
        expect(!events[index].empty(), "unaccepted batch suffix was consumed");
    }

    release_callback.count_down();
    instance.shutdown();
    const auto metrics = instance.metrics();
    expect(metrics.accepted == expected_accepted + 1U && metrics.rejected == 1U,
           "partial batch metrics mismatch");
    static_cast<void>(token);
}

void test_one_snapshot_per_worker_batch() {
    auto config = dispatcher_config{};
    config.queue_capacity = 8U;
    config.worker_count = 1U;
    config.worker_batch_size = 3U;
    dispatcher<int> instance{config};

    std::latch sentinel_entered{1};
    std::latch release_sentinel{1};
    std::atomic<std::size_t> original_calls{0U};
    std::atomic<std::size_t> late_calls{0U};
    std::optional<event_dispatcher::subscription> late;

    auto original = instance.subscribe([&](const int& event) {
        if (event == 0) {
            sentinel_entered.count_down();
            release_sentinel.wait();
            return;
        }
        original_calls.fetch_add(1U, std::memory_order_relaxed);
        if (event == 1) {
            late.emplace(instance.subscribe(
                [&](const int&) { late_calls.fetch_add(1U, std::memory_order_relaxed); }));
        }
    });

    expect(instance.publish(0) == queue_status::success, "sentinel publication failed");
    sentinel_entered.wait();
    std::array<int, 3> batch{1, 2, 3};
    const auto published = instance.try_publish_batch(std::span<int>{batch});
    expect(published.complete(), "snapshot test batch was not accepted");
    release_sentinel.count_down();

    instance.shutdown();
    expect(original_calls.load(std::memory_order_relaxed) == 3U,
           "original subscriber missed a batch event");
    expect(late_calls.load(std::memory_order_relaxed) == 0U,
           "subscriber added during a batch entered that batch snapshot");
    static_cast<void>(original);
}

} // namespace

int main() {
    test_queue_emplacement<bounded_mutex_queue>();
    test_queue_emplacement<bounded_lock_free_queue>();
    test_dispatcher_configuration_and_emplacement();
    test_empty_copy_and_closed_batches();
    test_partial_batch<bounded_mutex_queue>(1U, 1U);
    test_partial_batch<bounded_lock_free_queue>(2U, 2U);
    test_one_snapshot_per_worker_batch();
}
