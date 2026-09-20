#pragma once

#include "event_dispatcher/delivery.hpp"
#include "event_dispatcher/detail/subscription_control.hpp"

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace event_dispatcher::detail {

using subscriber_id = std::uint64_t;

// Owns callback lifetime plus the selected delivery policy. Registry snapshots
// retain this state through shared ownership, while synchronous unsubscribe
// closes callback entry and waits for every already-entered invocation.
template <typename Event>
class subscriber_state final : public subscription_control,
                               public std::enable_shared_from_this<subscriber_state<Event>> {
  public:
    using callback_type = std::function<void(const Event&)>;
    using retirement_callback = std::function<void()>;
    using error_handler = std::function<void(std::exception_ptr)>;

    subscriber_state(subscriber_id id, callback_type callback, retirement_callback retire,
                     subscription_options options, error_handler on_error, const void* owner_tag)
        : id_(id), callback_(std::move(callback)), retire_(std::move(retire)), options_(options),
          on_error_(std::move(on_error)), owner_tag_(owner_tag) {
        if (options_.slow_callback_threshold < std::chrono::nanoseconds::zero()) {
            throw std::invalid_argument{"slow callback threshold must not be negative"};
        }
        if (options_.delivery == delivery_policy::isolated) {
            if (options_.mailbox_capacity == 0U) {
                throw std::invalid_argument{
                    "isolated subscriber mailbox capacity must be non-zero"};
            }
            if constexpr (!std::copy_constructible<Event>) {
                throw std::invalid_argument{
                    "isolated delivery requires a copy-constructible Event"};
            }
        }
    }

    subscriber_state(const subscriber_state&) = delete;
    subscriber_state& operator=(const subscriber_state&) = delete;

    ~subscriber_state() override {
        if (options_.delivery != delivery_policy::isolated) {
            return;
        }
        {
            std::lock_guard lock{mailbox_mutex_};
            mailbox_accepting_ = false;
            executor_stopping_ = true;
            drain_mailbox_ = false;
            mailbox_.clear();
        }
        mailbox_ready_.notify_all();
        mailbox_space_.notify_all();

        if (executor_.joinable()) {
            if (executor_.get_id() == std::this_thread::get_id()) {
                // Self-unsubscribe can release the final external owner while
                // the executor's captured shared_ptr is unwinding.
                executor_.detach();
            } else {
                executor_.join();
            }
        }
    }

    void start_executor() {
        if (options_.delivery != delivery_policy::isolated) {
            return;
        }
        auto self = this->shared_from_this();
        executor_ = std::thread{[self = std::move(self)] { self->executor_loop(); }};
    }

    [[nodiscard]] subscriber_id id() const noexcept { return id_; }

    // Direct delivery may run concurrently or under this subscriber's serial
    // mutex. Isolated delivery only copies into the bounded mailbox; user code
    // runs on the dedicated executor.
    [[nodiscard]] bool try_invoke(const Event& event) {
        offered_.fetch_add(1U, std::memory_order_relaxed);
        if (options_.delivery == delivery_policy::isolated) {
            return enqueue_isolated(event);
        }
        if (!begin_invocation()) {
            rejected_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        }

        if (options_.delivery == delivery_policy::serialized) {
            std::lock_guard serial{serial_callback_mutex_};
            execute_callback(event);
        } else {
            execute_callback(event);
        }
        return true;
    }

    // Called by dispatcher shutdown. Drain processes every accepted mailbox
    // entry; discard destroys pending entries and wakes lossless producers.
    void stop_delivery(bool drain) noexcept {
        if (options_.delivery != delivery_policy::isolated) {
            return;
        }
        {
            std::lock_guard lock{mailbox_mutex_};
            mailbox_accepting_ = false;
            executor_stopping_ = true;
            drain_mailbox_ = drain_mailbox_ && drain;
            if (!drain) {
                dropped_.fetch_add(static_cast<std::uint64_t>(mailbox_.size()),
                                   std::memory_order_relaxed);
                mailbox_.clear();
            }
        }
        mailbox_ready_.notify_all();
        mailbox_space_.notify_all();

        std::lock_guard join_lock{executor_join_mutex_};
        if (executor_.joinable() && executor_.get_id() == std::this_thread::get_id()) {
            return;
        }
        if (executor_.joinable()) {
            executor_.join();
        }
    }

    void unsubscribe() noexcept override {
        retirement_callback retire;
        bool called_from_this_callback = false;
        {
            std::lock_guard lock{state_mutex_};
            active_ = false;
            called_from_this_callback = invoking_on_this_thread();
            if (!retirement_started_) {
                retirement_started_ = true;
                retire = std::move(retire_);
            }
        }

        if (retire) {
            try {
                retire();
            } catch (...) {
                // Logical deactivation is complete even if snapshot cleanup
                // cannot allocate. Future dispatch skips the inactive state.
            }
        }

        stop_delivery(false);
        if (called_from_this_callback) {
            return;
        }

        std::unique_lock lock{state_mutex_};
        no_callbacks_.wait(lock, [this] { return in_flight_ == 0U; });
    }

    [[nodiscard]] bool subscribed() const noexcept override {
        std::lock_guard lock{state_mutex_};
        return active_;
    }

    [[nodiscard]] subscription_metrics metrics() const noexcept override {
        return subscription_metrics{
            offered_.load(std::memory_order_relaxed),
            delivered_.load(std::memory_order_relaxed),
            dropped_.load(std::memory_order_relaxed),
            rejected_.load(std::memory_order_relaxed),
            callback_errors_.load(std::memory_order_relaxed),
            slow_callbacks_.load(std::memory_order_relaxed),
        };
    }

    [[nodiscard]] std::size_t in_flight() const {
        std::lock_guard lock{state_mutex_};
        return in_flight_;
    }

    [[nodiscard]] static bool callback_active_for(const void* owner_tag) noexcept {
        for (auto* context = current_invocation_; context != nullptr; context = context->previous) {
            if (context->state->owner_tag_ == owner_tag) {
                return true;
            }
        }
        return false;
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
        explicit invocation_guard(subscriber_state& state) noexcept
            : state_(state), started_(std::chrono::steady_clock::now()) {}
        invocation_guard(const invocation_guard&) = delete;
        invocation_guard& operator=(const invocation_guard&) = delete;
        ~invocation_guard() {
            state_.finish_invocation(std::chrono::steady_clock::now() - started_);
        }

      private:
        subscriber_state& state_;
        std::chrono::steady_clock::time_point started_;
    };

    [[nodiscard]] bool begin_invocation() {
        std::lock_guard lock{state_mutex_};
        if (!active_) {
            return false;
        }
        ++in_flight_;
        return true;
    }

    void finish_invocation(std::chrono::steady_clock::duration elapsed) noexcept {
        delivered_.fetch_add(1U, std::memory_order_relaxed);
        if (options_.slow_callback_threshold != std::chrono::nanoseconds::zero() &&
            elapsed >= options_.slow_callback_threshold) {
            slow_callbacks_.fetch_add(1U, std::memory_order_relaxed);
        }

        bool became_idle = false;
        {
            std::lock_guard lock{state_mutex_};
            --in_flight_;
            became_idle = in_flight_ == 0U;
        }
        if (became_idle) {
            no_callbacks_.notify_all();
        }
    }

    void invoke_callback(const Event& event) {
        try {
            callback_(event);
        } catch (...) {
            callback_errors_.fetch_add(1U, std::memory_order_relaxed);
            throw;
        }
    }

    void execute_callback(const Event& event) {
        invocation_guard completion{*this};
        invocation_scope current_callback{this};
        invoke_callback(event);
    }

    [[nodiscard]] bool enqueue_isolated(const Event& event) {
        if constexpr (!std::copy_constructible<Event>) {
            rejected_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        } else {
            std::unique_lock lock{mailbox_mutex_};
            if (options_.guarantee == delivery_guarantee::lossless) {
                mailbox_space_.wait(lock, [this] {
                    return !mailbox_accepting_ || mailbox_.size() < options_.mailbox_capacity;
                });
            } else if (mailbox_.size() == options_.mailbox_capacity) {
                dropped_.fetch_add(1U, std::memory_order_relaxed);
                return true;
            }

            if (!mailbox_accepting_) {
                rejected_.fetch_add(1U, std::memory_order_relaxed);
                return false;
            }
            try {
                mailbox_.push_back(event);
            } catch (...) {
                rejected_.fetch_add(1U, std::memory_order_relaxed);
                throw;
            }
            lock.unlock();
            mailbox_ready_.notify_one();
            return true;
        }
    }

    void executor_loop() noexcept {
        for (;;) {
            std::unique_lock lock{mailbox_mutex_};
            mailbox_ready_.wait(lock, [this] { return executor_stopping_ || !mailbox_.empty(); });
            if (executor_stopping_ && (!drain_mailbox_ || mailbox_.empty())) {
                return;
            }

            Event event{std::move(mailbox_.front())};
            mailbox_.pop_front();
            lock.unlock();
            mailbox_space_.notify_one();

            if (!begin_invocation()) {
                dropped_.fetch_add(1U, std::memory_order_relaxed);
                continue;
            }

            try {
                execute_callback(event);
            } catch (...) {
                report_error(std::current_exception());
            }
        }
    }

    void report_error(std::exception_ptr error) noexcept {
        if (!on_error_) {
            return;
        }
        try {
            on_error_(std::move(error));
        } catch (...) {
            // Error handlers cannot be allowed to terminate the executor.
        }
    }

    [[nodiscard]] bool invoking_on_this_thread() const noexcept {
        for (auto* context = current_invocation_; context != nullptr; context = context->previous) {
            if (context->state == this) {
                return true;
            }
        }
        return false;
    }

    inline static thread_local invocation_context* current_invocation_{nullptr};

    const subscriber_id id_;
    const callback_type callback_;
    retirement_callback retire_;
    const subscription_options options_;
    const error_handler on_error_;
    const void* const owner_tag_;

    mutable std::mutex state_mutex_;
    std::condition_variable no_callbacks_;
    std::mutex serial_callback_mutex_;
    std::size_t in_flight_{0U};
    bool active_{true};
    bool retirement_started_{false};

    std::mutex mailbox_mutex_;
    std::condition_variable mailbox_ready_;
    std::condition_variable mailbox_space_;
    std::deque<Event> mailbox_;
    bool mailbox_accepting_{true};
    bool executor_stopping_{false};
    bool drain_mailbox_{true};
    std::mutex executor_join_mutex_;
    std::thread executor_;

    std::atomic<std::uint64_t> offered_{0U};
    std::atomic<std::uint64_t> delivered_{0U};
    std::atomic<std::uint64_t> dropped_{0U};
    std::atomic<std::uint64_t> rejected_{0U};
    std::atomic<std::uint64_t> callback_errors_{0U};
    std::atomic<std::uint64_t> slow_callbacks_{0U};
};

} // namespace event_dispatcher::detail
