#pragma once

#include "event_dispatcher/batch_publish_result.hpp"
#include "event_dispatcher/config.hpp"
#include "event_dispatcher/execution/worker_pool.hpp"
#include "event_dispatcher/lifecycle.hpp"
#include "event_dispatcher/metrics.hpp"
#include "event_dispatcher/queue/bounded_mutex_queue.hpp"
#include "event_dispatcher/queue/concurrent_queue.hpp"
#include "event_dispatcher/queue/queue_status.hpp"
#include "event_dispatcher/registry/snapshot_registry.hpp"
#include "event_dispatcher/subscription/subscription.hpp"

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <utility>
#include <vector>

namespace event_dispatcher {

// Asynchronous dispatcher whose queue policy can select the understandable
// mutex baseline or the bounded lock-free MPMC implementation.
template <typename Event, template <typename> class QueuePolicy = queue::bounded_mutex_queue>
class dispatcher final {
  public:
    using event_type = Event;
    using queue_type = QueuePolicy<Event>;
    using callback_type = std::function<void(const Event&)>;
    using error_handler = std::function<void(std::exception_ptr)>;

    static_assert(queue::concurrent_queue<queue_type, Event>,
                  "QueuePolicy must satisfy event_dispatcher::queue::concurrent_queue");

    explicit dispatcher(dispatcher_config config = {}, error_handler on_error = {})
        : state_(make_state(config, std::move(on_error))), shutdown_policy_(config.shutdown) {
        // Workers capture shared state rather than this. The complete state is
        // constructed before the first thread starts, and stays alive until the
        // last worker exits even while dispatcher destruction is in progress.
        workers_.start(config.worker_count,
                       [state = state_](std::stop_token stop, std::size_t worker_index) {
                           state->run(stop, worker_index);
                       });
        state_->lifecycle.store(lifecycle_state::running, std::memory_order_release);
    }

    dispatcher(const dispatcher&) = delete;
    dispatcher& operator=(const dispatcher&) = delete;
    dispatcher(dispatcher&&) = delete;
    dispatcher& operator=(dispatcher&&) = delete;

    ~dispatcher() noexcept {
        try {
            shutdown();
        } catch (...) {
            std::terminate();
        }
    }

    [[nodiscard]] subscription subscribe(callback_type callback) {
        std::lock_guard lock{lifecycle_mutex_};
        if (state_->lifecycle.load(std::memory_order_relaxed) != lifecycle_state::running) {
            throw std::logic_error{"cannot subscribe after dispatcher shutdown begins"};
        }
        return state_->subscribers.subscribe(std::move(callback));
    }

    [[nodiscard]] queue::queue_status try_publish(Event&& event) {
        return publish_with([&] { return state_->events.try_push(std::move(event)); });
    }

    [[nodiscard]] queue::queue_status try_publish(const Event& event)
        requires std::copy_constructible<Event>
    {
        return publish_with([&] { return state_->events.try_push(event); });
    }

    // Construct the Event directly in queue storage. This removes the Event
    // temporary and its move, but construction can still throw. Queue policies
    // must restore the reserved slot before propagating that exception.
    template <typename... Args>
        requires std::constructible_from<Event, Args...> &&
                 queue::emplacing_queue<queue_type, Args...>
    [[nodiscard]] queue::queue_status try_emplace(Args&&... args) {
        return publish_with(
            [&] { return state_->events.try_emplace(std::forward<Args>(args)...); });
    }

    // Partial success is an ordered prefix: processing stops at the first
    // rejected event. Unaccepted move-source events remain untouched because
    // each queue try_push checks capacity and lifecycle before moving.
    [[nodiscard]] batch_publish_result try_publish_batch(std::span<Event> events) {
        std::size_t accepted = 0U;
        for (auto& event : events) {
            const auto status = try_publish(std::move(event));
            if (status != queue::queue_status::success) {
                return batch_publish_result{events.size(), accepted, status};
            }
            ++accepted;
        }
        return batch_publish_result{events.size(), accepted, queue::queue_status::success};
    }

    [[nodiscard]] batch_publish_result try_publish_batch(std::span<const Event> events)
        requires std::copy_constructible<Event>
    {
        std::size_t accepted = 0U;
        for (const auto& event : events) {
            const auto status = try_publish(event);
            if (status != queue::queue_status::success) {
                return batch_publish_result{events.size(), accepted, status};
            }
            ++accepted;
        }
        return batch_publish_result{events.size(), accepted, queue::queue_status::success};
    }

    [[nodiscard]] queue::queue_status publish(Event&& event, std::stop_token stop = {}) {
        return publish_with([&] { return state_->events.wait_push(std::move(event), stop); });
    }

    [[nodiscard]] queue::queue_status publish(const Event& event, std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return publish_with([&] { return state_->events.wait_push(event, stop); });
    }

    [[nodiscard]] queue::queue_status publish_until(Event&& event,
                                                    std::chrono::steady_clock::time_point deadline,
                                                    std::stop_token stop = {}) {
        return publish_with(
            [&] { return state_->events.wait_push_until(std::move(event), deadline, stop); });
    }

    [[nodiscard]] queue::queue_status publish_until(const Event& event,
                                                    std::chrono::steady_clock::time_point deadline,
                                                    std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return publish_with([&] { return state_->events.wait_push_until(event, deadline, stop); });
    }

    template <typename Rep, typename Period>
    [[nodiscard]] queue::queue_status publish_for(Event&& event,
                                                  std::chrono::duration<Rep, Period> timeout,
                                                  std::stop_token stop = {}) {
        const auto bounded_timeout =
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
        return publish_until(std::move(event), std::chrono::steady_clock::now() + bounded_timeout,
                             stop);
    }

    template <typename Rep, typename Period>
    [[nodiscard]] queue::queue_status publish_for(const Event& event,
                                                  std::chrono::duration<Rep, Period> timeout,
                                                  std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        const auto bounded_timeout =
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
        return publish_until(event, std::chrono::steady_clock::now() + bounded_timeout, stop);
    }

    // Drain shutdown linearizes acceptance at queue.close(): events accepted
    // before close are processed, while later publications return closed.
    // Calling shutdown (or destroying the dispatcher) from its own callback is
    // rejected because a worker cannot wait for itself to finish.
    void shutdown() { shutdown(shutdown_policy_); }

    void shutdown(shutdown_policy policy) {
        if (workers_.is_worker_thread()) {
            throw std::logic_error{"dispatcher shutdown cannot run inside its callback"};
        }

        {
            std::unique_lock lock{lifecycle_mutex_};
            const auto current = state_->lifecycle.load(std::memory_order_relaxed);
            if (current == lifecycle_state::stopped) {
                return;
            }
            if (current == lifecycle_state::drain_stopping ||
                current == lifecycle_state::discard_stopping) {
                lifecycle_changed_.wait(lock, [this] {
                    return state_->lifecycle.load(std::memory_order_relaxed) ==
                           lifecycle_state::stopped;
                });
                return;
            }

            const auto stopping = policy == shutdown_policy::drain
                                      ? lifecycle_state::drain_stopping
                                      : lifecycle_state::discard_stopping;
            state_->lifecycle.store(stopping, std::memory_order_release);
        }

        if (policy == shutdown_policy::drain) {
            state_->events.close();
        } else {
            const auto discarded = state_->events.close_and_discard();
            state_->dropped.fetch_add(static_cast<std::uint64_t>(discarded),
                                      std::memory_order_relaxed);
        }
        workers_.join();
        {
            std::lock_guard lock{lifecycle_mutex_};
            state_->lifecycle.store(lifecycle_state::stopped, std::memory_order_release);
        }
        lifecycle_changed_.notify_all();
    }

    [[nodiscard]] std::size_t worker_count() const noexcept { return workers_.size(); }
    [[nodiscard]] std::size_t subscriber_count() const {
        return state_->subscribers.active_count();
    }
    [[nodiscard]] lifecycle_state lifecycle() const noexcept {
        return state_->lifecycle.load(std::memory_order_acquire);
    }
    [[nodiscard]] dispatcher_metrics metrics() const noexcept {
        return dispatcher_metrics{
            state_->accepted.load(std::memory_order_relaxed),
            state_->rejected.load(std::memory_order_relaxed),
            state_->dropped.load(std::memory_order_relaxed),
        };
    }

  private:
    struct shared_state final {
        shared_state(std::size_t capacity, std::size_t worker_count, std::size_t batch_size,
                     error_handler handler)
            : events(capacity), worker_batch_size(batch_size), worker_batches(worker_count),
              on_error(std::move(handler)) {
            // Perform every dispatcher-owned batch allocation before starting
            // a worker. Allocation failure then propagates from construction
            // instead of escaping a thread entry function and terminating.
            for (auto& batch : worker_batches) {
                batch.reserve(worker_batch_size);
            }
        }

        void run(std::stop_token stop, std::size_t worker_index) {
            // Each worker exclusively owns its preallocated vector and reuses
            // it for the lifetime of the processing loop.
            auto& batch = worker_batches[worker_index];

            for (;;) {
                auto result = events.wait_pop(stop);
                if (result.status() == queue::queue_status::success) {
                    batch.clear();
                    batch.emplace_back(std::move(result).value());

                    while (batch.size() < worker_batch_size) {
                        auto next = events.try_pop();
                        if (next.status() != queue::queue_status::success) {
                            break;
                        }
                        batch.emplace_back(std::move(next).value());
                    }

                    broadcast_batch(batch);
                    continue;
                }
                if (result.status() == queue::queue_status::closed ||
                    result.status() == queue::queue_status::stopped) {
                    return;
                }
            }
        }

        void broadcast_batch(const std::vector<Event>& batch) noexcept {
            // Snapshot time is after this complete batch has been dequeued and
            // immediately before its first callback. Every event in the batch
            // uses the same immutable membership snapshot. An unsubscribe still
            // prevents later callback entry through subscriber_state::try_invoke.
            const auto snapshot = subscribers.acquire_snapshot();
            for (const auto& event : batch) {
                for (const auto& subscriber : *snapshot) {
                    try {
                        static_cast<void>(subscriber->try_invoke(event));
                    } catch (...) {
                        report_error(std::current_exception());
                    }
                }
            }
        }

        void report_error(std::exception_ptr error) noexcept {
            if (!on_error) {
                return;
            }
            try {
                on_error(std::move(error));
            } catch (...) {
                // Error reporting must not terminate a worker or prevent later
                // subscribers from receiving the event.
            }
        }

        queue_type events;
        registry::snapshot_registry<Event> subscribers;
        const std::size_t worker_batch_size;
        std::vector<std::vector<Event>> worker_batches;
        const error_handler on_error;
        std::atomic<lifecycle_state> lifecycle{lifecycle_state::created};
        std::atomic<std::uint64_t> accepted{0U};
        std::atomic<std::uint64_t> rejected{0U};
        std::atomic<std::uint64_t> dropped{0U};
    };

    template <typename Operation>
    [[nodiscard]] queue::queue_status publish_with(Operation&& operation) {
        // This load is an early rejection optimization, not the acceptance
        // linearization point. Queue close and push share the queue mutex, so a
        // racing operation is either accepted before close or rejected by it.
        if (state_->lifecycle.load(std::memory_order_acquire) != lifecycle_state::running) {
            state_->rejected.fetch_add(1U, std::memory_order_relaxed);
            return queue::queue_status::closed;
        }

        const auto result = std::forward<Operation>(operation)();
        auto& counter =
            result == queue::queue_status::success ? state_->accepted : state_->rejected;
        counter.fetch_add(1U, std::memory_order_relaxed);
        return result;
    }

    [[nodiscard]] static std::shared_ptr<shared_state> make_state(const dispatcher_config& config,
                                                                  error_handler handler) {
        if (config.worker_count == 0U) {
            throw std::invalid_argument{"dispatcher worker_count must be non-zero"};
        }
        if (config.worker_batch_size == 0U) {
            throw std::invalid_argument{"dispatcher worker_batch_size must be non-zero"};
        }
        return std::make_shared<shared_state>(config.queue_capacity, config.worker_count,
                                              config.worker_batch_size, std::move(handler));
    }

    std::shared_ptr<shared_state> state_;
    execution::worker_pool workers_;
    const shutdown_policy shutdown_policy_;
    std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_changed_;
};

} // namespace event_dispatcher
