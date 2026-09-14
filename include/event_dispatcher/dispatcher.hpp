#pragma once

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
#include <stdexcept>
#include <stop_token>
#include <utility>

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
                       [state = state_](std::stop_token stop, std::size_t) { state->run(stop); });
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
        shared_state(std::size_t capacity, error_handler handler)
            : events(capacity), on_error(std::move(handler)) {}

        void run(std::stop_token stop) {
            for (;;) {
                auto result = events.wait_pop(stop);
                if (result.status() == queue::queue_status::success) {
                    broadcast(result.value());
                    continue;
                }
                if (result.status() == queue::queue_status::closed ||
                    result.status() == queue::queue_status::stopped) {
                    return;
                }
            }
        }

        void broadcast(const Event& event) noexcept {
            // Snapshot time is after dequeue and immediately before iteration.
            // One worker owns this event and invokes every subscriber in that
            // immutable snapshot. Other workers may broadcast other events at
            // the same time, so cross-event callback order is not guaranteed.
            const auto snapshot = subscribers.acquire_snapshot();
            for (const auto& subscriber : *snapshot) {
                try {
                    static_cast<void>(subscriber->try_invoke(event));
                } catch (...) {
                    report_error(std::current_exception());
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
        return std::make_shared<shared_state>(config.queue_capacity, std::move(handler));
    }

    std::shared_ptr<shared_state> state_;
    execution::worker_pool workers_;
    const shutdown_policy shutdown_policy_;
    std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_changed_;
};

} // namespace event_dispatcher
