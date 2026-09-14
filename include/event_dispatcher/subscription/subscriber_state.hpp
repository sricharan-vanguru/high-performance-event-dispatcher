#pragma once

#include "event_dispatcher/detail/subscription_control.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <utility>

namespace event_dispatcher::detail {

using subscriber_id = std::uint64_t;

// Owns one callback and the synchronization required to retire it safely.
// Registry snapshots keep this control block alive with shared ownership. The
// subscriber's application object is a separate lifetime concern; a dedicated
// weak-ownership helper can address that object lifetime independently.
template <typename Event> class subscriber_state final : public subscription_control {
  public:
    using callback_type = std::function<void(const Event&)>;
    using retirement_callback = std::function<void()>;

    subscriber_state(subscriber_id id, callback_type callback, retirement_callback retire)
        : id_(id), callback_(std::move(callback)), retire_(std::move(retire)) {}

    subscriber_state(const subscriber_state&) = delete;
    subscriber_state& operator=(const subscriber_state&) = delete;

    [[nodiscard]] subscriber_id id() const noexcept { return id_; }

    // Returns false when logical unsubscription has already prevented entry.
    // Callback exceptions intentionally propagate to the future dispatcher
    // error policy, but invocation_guard still releases the in-flight count.
    [[nodiscard]] bool try_invoke(const Event& event) {
        {
            std::lock_guard lock{mutex_};
            if (!active_) {
                return false;
            }

            // active_ and in_flight_ share one mutex. Therefore unsubscribe can
            // either deactivate before this increment (we skip), or observe the
            // increment and wait. There is no unprotected check/increment gap.
            ++in_flight_;
        }

        invocation_guard completion{*this};
        invocation_scope current_callback{this};
        callback_(event);
        return true;
    }

    void unsubscribe() noexcept override {
        retirement_callback retire;
        bool called_from_this_callback = false;

        {
            std::lock_guard lock{mutex_};

            // This is the logical-unsubscribe linearization point. Any callback
            // entry that obtains mutex_ afterward sees inactive and must skip.
            active_ = false;
            called_from_this_callback = invoking_on_this_thread();

            // Only one racing unsubscribe physically removes the registry
            // entry. Moving std::function is noexcept and avoids allocating in
            // this noexcept RAII path.
            if (!retirement_started_) {
                retirement_started_ = true;
                retire = std::move(retire_);
            }
        }

        // Registry modification may copy a snapshot, so never perform it while
        // holding the subscriber-state mutex. The callback is already inactive;
        // an old snapshot can retain state but cannot enter it.
        if (retire) {
            try {
                retire();
            } catch (...) {
                // RAII destruction cannot report allocation failure. Logical
                // deactivation is still complete; a tombstone may remain in an
                // old/current snapshot and will be ignored safely.
            }
        }

        if (called_from_this_callback) {
            // Waiting here would deadlock on our own in-flight count. The guard
            // in try_invoke completes retirement as the callback unwinds.
            return;
        }

        std::unique_lock lock{mutex_};
        no_callbacks_.wait(lock, [this] { return in_flight_ == 0U; });
    }

    [[nodiscard]] bool subscribed() const noexcept override {
        std::lock_guard lock{mutex_};
        return active_;
    }

    [[nodiscard]] std::size_t in_flight() const {
        std::lock_guard lock{mutex_};
        return in_flight_;
    }

  private:
    struct invocation_context {
        const subscriber_state* state;
        invocation_context* previous;
    };

    class invocation_scope final {
      public:
        explicit invocation_scope(const subscriber_state* state) noexcept
            : context_{state, current_invocation_} {
            current_invocation_ = &context_;
        }

        invocation_scope(const invocation_scope&) = delete;
        invocation_scope& operator=(const invocation_scope&) = delete;

        ~invocation_scope() { current_invocation_ = context_.previous; }

      private:
        invocation_context context_;
    };

    class invocation_guard final {
      public:
        explicit invocation_guard(subscriber_state& state) noexcept : state_(state) {}

        invocation_guard(const invocation_guard&) = delete;
        invocation_guard& operator=(const invocation_guard&) = delete;

        ~invocation_guard() { state_.finish_invocation(); }

      private:
        subscriber_state& state_;
    };

    [[nodiscard]] bool invoking_on_this_thread() const noexcept {
        // A linked stack stored in thread-local storage handles nested dispatch
        // without heap allocation on callback entry.
        for (auto* context = current_invocation_; context != nullptr; context = context->previous) {
            if (context->state == this) {
                return true;
            }
        }
        return false;
    }

    void finish_invocation() noexcept {
        bool became_idle = false;
        {
            std::lock_guard lock{mutex_};
            --in_flight_;
            became_idle = in_flight_ == 0U;
        }

        if (became_idle) {
            no_callbacks_.notify_all();
        }
    }

    inline static thread_local invocation_context* current_invocation_{nullptr};

    const subscriber_id id_;
    const callback_type callback_;

    mutable std::mutex mutex_;
    std::condition_variable no_callbacks_;
    retirement_callback retire_;
    std::size_t in_flight_{0U};
    bool active_{true};
    bool retirement_started_{false};
};

} // namespace event_dispatcher::detail
