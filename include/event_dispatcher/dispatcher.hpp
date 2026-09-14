#pragma once

#include "event_dispatcher/config.hpp"
#include "event_dispatcher/execution/worker_pool.hpp"
#include "event_dispatcher/queue/bounded_mutex_queue.hpp"
#include "event_dispatcher/queue/queue_status.hpp"
#include "event_dispatcher/registry/snapshot_registry.hpp"
#include "event_dispatcher/subscription/subscription.hpp"

#include <cstddef>
#include <concepts>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <utility>

namespace event_dispatcher {

// Correctness-first asynchronous dispatcher. QueuePolicy is the first hot-path
// policy seam; later phases can supply a lock-free queue with the same contract.
template <typename Event, template <typename> class QueuePolicy = queue::bounded_mutex_queue>
class dispatcher final {
public:
    using event_type = Event;
    using queue_type = QueuePolicy<Event>;
    using callback_type = std::function<void(const Event&)>;
    using error_handler = std::function<void(std::exception_ptr)>;

    explicit dispatcher(dispatcher_config config = {}, error_handler on_error = {})
        : state_(make_state(config, std::move(on_error))) {
        // Workers capture shared state rather than this. The complete state is
        // constructed before the first thread starts, and stays alive until the
        // last worker exits even while dispatcher destruction is in progress.
        workers_.start(config.worker_count,
                       [state = state_](std::stop_token stop, std::size_t) {
                           state->run(stop);
                       });
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
        return state_->subscribers.subscribe(std::move(callback));
    }

    [[nodiscard]] queue::queue_status try_publish(Event&& event) {
        return state_->events.try_push(std::move(event));
    }

    [[nodiscard]] queue::queue_status try_publish(const Event& event)
        requires std::copy_constructible<Event>
    {
        return state_->events.try_push(event);
    }

    [[nodiscard]] queue::queue_status publish(Event&& event, std::stop_token stop = {}) {
        return state_->events.wait_push(std::move(event), stop);
    }

    [[nodiscard]] queue::queue_status publish(const Event& event,
                                              std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return state_->events.wait_push(event, stop);
    }

    // Drain shutdown linearizes acceptance at queue.close(): events accepted
    // before close are processed, while later publications return closed.
    // Calling shutdown (or destroying the dispatcher) from its own callback is
    // rejected because a worker cannot wait for itself to finish.
    void shutdown() {
        if (workers_.is_worker_thread()) {
            throw std::logic_error{"dispatcher shutdown cannot run inside its callback"};
        }

        std::lock_guard lock{shutdown_mutex_};
        state_->events.close();
        workers_.join();
    }

    [[nodiscard]] std::size_t worker_count() const noexcept { return workers_.size(); }
    [[nodiscard]] std::size_t subscriber_count() const {
        return state_->subscribers.active_count();
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
    };

    [[nodiscard]] static std::shared_ptr<shared_state>
    make_state(const dispatcher_config& config, error_handler handler) {
        if (config.worker_count == 0U) {
            throw std::invalid_argument{"dispatcher worker_count must be non-zero"};
        }
        if (config.shutdown != shutdown_policy::drain) {
            throw std::invalid_argument{"discard shutdown is planned for Phase 4"};
        }
        if (config.callback_mode != callback_concurrency::concurrent) {
            throw std::invalid_argument{"serialized callbacks are planned for Phase 8"};
        }
        return std::make_shared<shared_state>(config.queue_capacity, std::move(handler));
    }

    std::shared_ptr<shared_state> state_;
    execution::worker_pool workers_;
    std::mutex shutdown_mutex_;
};

} // namespace event_dispatcher
