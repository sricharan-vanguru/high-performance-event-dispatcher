#pragma once

#include "event_dispatcher/subscription/subscriber_state.hpp"
#include "event_dispatcher/subscription/subscription.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace event_dispatcher::registry {

// Copy-on-write subscriber registry optimized for frequent reads and infrequent
// membership changes. Dispatch loads one immutable snapshot and never takes the
// writer mutex. This is low-lock rather than universally lock-free because the
// standard does not require atomic<shared_ptr<T>> to be lock-free.
template <typename Event> class snapshot_registry final {
  public:
    using subscriber_type = detail::subscriber_state<Event>;
    using subscriber_ptr = std::shared_ptr<subscriber_type>;
    using subscriber_list = std::vector<subscriber_ptr>;
    using snapshot = std::shared_ptr<const subscriber_list>;
    using callback_type = typename subscriber_type::callback_type;
    using error_handler = typename subscriber_type::error_handler;

    explicit snapshot_registry(error_handler on_error = {})
        : core_(std::make_shared<registry_core>(std::move(on_error))) {}

    snapshot_registry(const snapshot_registry&) = delete;
    snapshot_registry& operator=(const snapshot_registry&) = delete;
    snapshot_registry(snapshot_registry&&) = delete;
    snapshot_registry& operator=(snapshot_registry&&) = delete;
    ~snapshot_registry() { core_->stop_delivery(false); }

    [[nodiscard]] subscription subscribe(callback_type callback,
                                         subscription_options options = {}) {
        if (!callback) {
            throw std::invalid_argument{"snapshot_registry callback must not be empty"};
        }
        return core_->subscribe(std::move(callback), options);
    }

    // Acquire is the complete dispatch-side registry operation. The returned
    // immutable snapshot owns every state it exposes, so it remains valid even
    // if another thread publishes a newer list or destroys the facade.
    [[nodiscard]] snapshot acquire_snapshot() const noexcept {
        return core_->current.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t active_count() const {
        const auto current = acquire_snapshot();
        return static_cast<std::size_t>(
            std::count_if(current->begin(), current->end(), [](const subscriber_ptr& subscriber) {
                return subscriber->subscribed();
            }));
    }

    void stop_delivery(bool drain) noexcept { core_->stop_delivery(drain); }

    [[nodiscard]] bool callback_active_on_this_thread() const noexcept {
        return subscriber_type::callback_active_for(core_.get());
    }

  private:
    struct registry_core final : std::enable_shared_from_this<registry_core> {
        explicit registry_core(error_handler handler)
            : current(std::make_shared<const subscriber_list>()), on_error(std::move(handler)) {}

        [[nodiscard]] subscription subscribe(callback_type callback, subscription_options options) {
            std::lock_guard writer_lock{writer_mutex};

            const detail::subscriber_id id = allocate_id();
            std::weak_ptr<registry_core> weak_core = this->weak_from_this();
            auto state = std::make_shared<subscriber_type>(
                id, std::move(callback),
                [weak_core, id] {
                    if (auto core = weak_core.lock()) {
                        core->retire(id);
                    }
                },
                options, on_error, this);

            const auto previous = current.load(std::memory_order_relaxed);
            auto next = std::make_shared<subscriber_list>();
            next->reserve(previous->size() + 1U);

            // Filtering inactive tombstones also repairs the rare case where a
            // previous noexcept unsubscribe could not allocate a removal copy.
            for (const auto& subscriber : *previous) {
                if (subscriber->subscribed()) {
                    next->push_back(subscriber);
                }
            }
            next->push_back(state);

            // Start only after every potentially throwing snapshot allocation.
            // Once the executor exists, publishing the prepared snapshot is a
            // noexcept atomic shared_ptr store.
            state->start_executor();
            current.store(std::move(next), std::memory_order_release);
            return subscription{std::move(state)};
        }

        void stop_delivery(bool drain) noexcept {
            const auto snapshot = current.load(std::memory_order_acquire);
            for (const auto& subscriber : *snapshot) {
                subscriber->stop_delivery(drain);
            }
        }

        void retire(detail::subscriber_id id) {
            std::lock_guard writer_lock{writer_mutex};
            const auto previous = current.load(std::memory_order_relaxed);

            const auto found = std::find_if(
                previous->begin(), previous->end(),
                [id](const subscriber_ptr& subscriber) { return subscriber->id() == id; });
            if (found == previous->end()) {
                return;
            }

            auto next = std::make_shared<subscriber_list>();
            next->reserve(previous->size() - 1U);
            for (const auto& subscriber : *previous) {
                if (subscriber->id() != id && subscriber->subscribed()) {
                    next->push_back(subscriber);
                }
            }

            // Release publishes a fully constructed immutable list. Readers use
            // acquire so subscriber-state construction is visible before use.
            current.store(std::move(next), std::memory_order_release);
        }

        [[nodiscard]] detail::subscriber_id allocate_id() {
            // Reserve zero as invalid and stop before unsigned wrap could reuse
            // an identifier still present in an old snapshot.
            if (next_id_ == std::numeric_limits<detail::subscriber_id>::max()) {
                throw std::overflow_error{"snapshot_registry subscriber id exhausted"};
            }
            return next_id_++;
        }

        std::atomic<snapshot> current;
        const error_handler on_error;
        std::mutex writer_mutex;
        detail::subscriber_id next_id_{1U};
    };

    std::shared_ptr<registry_core> core_;
};

} // namespace event_dispatcher::registry
