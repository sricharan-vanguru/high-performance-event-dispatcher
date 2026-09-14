#pragma once

#include "event_dispatcher/detail/cache_line.hpp"
#include "event_dispatcher/queue/detail/scalable_circular_index_queue.hpp"
#include "event_dispatcher/queue/pop_result.hpp"
#include "event_dispatcher/queue/queue_status.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <stop_token>
#include <type_traits>
#include <utility>

namespace event_dispatcher::queue {

// Generic Event wrapper around two lock-free SCQ index rings. One ring contains
// available event-slot indices and the other contains published event indices.
// Event storage is allocated once and never moved as a container.
template <typename Event> class bounded_lock_free_queue final {
  public:
    static_assert(std::is_nothrow_move_constructible_v<Event>,
                  "bounded_lock_free_queue requires a non-throwing Event move constructor");
    static_assert(std::is_nothrow_destructible_v<Event>,
                  "bounded_lock_free_queue requires a non-throwing Event destructor");

    explicit bounded_lock_free_queue(std::size_t capacity)
        : capacity_(validate_capacity(capacity)), slots_(std::make_unique<slot[]>(capacity_)),
          available_indices_(capacity_, true), published_indices_(capacity_, false) {}

    bounded_lock_free_queue(const bounded_lock_free_queue&) = delete;
    bounded_lock_free_queue& operator=(const bounded_lock_free_queue&) = delete;
    bounded_lock_free_queue(bounded_lock_free_queue&&) = delete;
    bounded_lock_free_queue& operator=(bounded_lock_free_queue&&) = delete;

    ~bounded_lock_free_queue() noexcept {
        // Destruction must not race with operations. Only published slots own
        // Events; indices held by incomplete producers are excluded by contract.
        while (const auto index = published_indices_.try_dequeue(false)) {
            std::destroy_at(slots_[*index].event());
        }
    }

    [[nodiscard]] queue_status try_push(Event&& event) { return try_push_impl(std::move(event)); }

    [[nodiscard]] queue_status try_push(const Event& event)
        requires std::copy_constructible<Event>
    {
        return try_push_impl(event);
    }

    [[nodiscard]] queue_status wait_push(Event&& event, std::stop_token stop = {}) {
        return wait_push_impl(std::move(event), stop);
    }

    [[nodiscard]] queue_status wait_push(const Event& event, std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return wait_push_impl(event, stop);
    }

    [[nodiscard]] queue_status wait_push_until(Event&& event,
                                               std::chrono::steady_clock::time_point deadline,
                                               std::stop_token stop = {}) {
        return wait_push_until_impl(std::move(event), deadline, stop);
    }

    [[nodiscard]] queue_status wait_push_until(const Event& event,
                                               std::chrono::steady_clock::time_point deadline,
                                               std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return wait_push_until_impl(event, deadline, stop);
    }

    [[nodiscard]] pop_result<Event> try_pop() {
        operation_guard consumer{consumer_gate_};
        if (!consumer) {
            return pop_result<Event>::closed();
        }

        const auto index = published_indices_.try_dequeue(false);
        if (!index) {
            return producer_quiesced_.load(std::memory_order_acquire) ? pop_result<Event>::closed()
                                                                      : pop_result<Event>::empty();
        }

        auto& source = slots_[*index];
        Event event{std::move(*source.event())};
        std::destroy_at(source.event());
        occupied_count_.value.fetch_sub(1U, std::memory_order_relaxed);
        // The free-index ring may have reached its empty fast-path threshold,
        // so returning capacity must re-arm that threshold.
        available_indices_.enqueue(*index, false);
        signal_capacity();
        return pop_result<Event>::success(std::move(event));
    }

    [[nodiscard]] pop_result<Event> wait_pop(std::stop_token stop = {}) {
        const auto wake_waiter = [this] {
            data_epoch_.fetch_add(1U, std::memory_order_release);
            data_epoch_.notify_all();
        };
        std::stop_callback callback{stop, wake_waiter};

        while (true) {
            auto result = try_pop();
            if (result.status() != queue_status::empty) {
                return result;
            }
            if (stop.stop_requested()) {
                return pop_result<Event>::stopped();
            }

            const auto epoch = data_epoch_.load(std::memory_order_acquire);
            result = try_pop();
            if (result.status() != queue_status::empty) {
                return result;
            }
            if (stop.stop_requested()) {
                return pop_result<Event>::stopped();
            }
            data_epoch_.wait(epoch, std::memory_order_acquire);
        }
    }

    void close() {
        std::lock_guard transition{close_mutex_};
        close_producers();
    }

    [[nodiscard]] std::size_t close_and_discard() {
        std::lock_guard transition{close_mutex_};
        close_producers();
        close_gate(consumer_gate_);

        std::size_t discarded = 0U;
        while (const auto index = published_indices_.try_dequeue(false)) {
            std::destroy_at(slots_[*index].event());
            occupied_count_.value.fetch_sub(1U, std::memory_order_relaxed);
            available_indices_.enqueue(*index, false);
            ++discarded;
        }
        signal_data();
        signal_capacity();
        return discarded;
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] std::size_t size() const noexcept {
        // A diagnostic snapshot only. It can change immediately and must not be
        // used for a check-then-act protocol.
        return occupied_count_.value.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool closed() const noexcept {
        return (producer_gate_.load(std::memory_order_acquire) & closed_bit) != 0U;
    }

    [[nodiscard]] bool data_path_is_lock_free() const noexcept {
        return available_indices_.atomics_are_lock_free() &&
               published_indices_.atomics_are_lock_free() && occupied_count_.value.is_lock_free() &&
               producer_gate_.is_lock_free() && consumer_gate_.is_lock_free() &&
               producer_quiesced_.is_lock_free() && data_epoch_.is_lock_free() &&
               capacity_epoch_.is_lock_free();
    }

  private:
    static constexpr std::size_t closed_bit = std::size_t{1}
                                              << (std::numeric_limits<std::size_t>::digits - 1U);
    static constexpr std::size_t active_mask = closed_bit - 1U;

    struct alignas(event_dispatcher::detail::destructive_interference_size) slot final {
        alignas(Event) std::byte storage[sizeof(Event)];

        [[nodiscard]] Event* event() noexcept {
            return std::launder(reinterpret_cast<Event*>(storage));
        }
    };

    struct alignas(event_dispatcher::detail::destructive_interference_size) padded_count final {
        std::atomic<std::size_t> value{0U};
    };

    class operation_guard final {
      public:
        explicit operation_guard(std::atomic<std::size_t>& gate) noexcept : gate_(&gate) {
            auto state = gate_->load(std::memory_order_acquire);
            while ((state & closed_bit) == 0U) {
                if ((state & active_mask) == active_mask) {
                    std::terminate();
                }
                if (gate_->compare_exchange_weak(state, state + 1U, std::memory_order_acquire,
                                                 std::memory_order_relaxed)) {
                    entered_ = true;
                    return;
                }
            }
        }

        operation_guard(const operation_guard&) = delete;
        operation_guard& operator=(const operation_guard&) = delete;

        ~operation_guard() {
            if (entered_) {
                gate_->fetch_sub(1U, std::memory_order_release);
                gate_->notify_all();
            }
        }

        [[nodiscard]] explicit operator bool() const noexcept { return entered_; }

      private:
        std::atomic<std::size_t>* gate_;
        bool entered_{false};
    };

    [[nodiscard]] static std::size_t validate_capacity(std::size_t capacity) {
        constexpr auto largest_capacity =
            static_cast<std::size_t>(std::numeric_limits<std::intptr_t>::max()) / 3U;
        if (capacity < 2U || !std::has_single_bit(capacity)) {
            throw std::invalid_argument{
                "bounded_lock_free_queue capacity must be a power of two and at least two"};
        }
        if (capacity > largest_capacity) {
            throw std::invalid_argument{"bounded_lock_free_queue capacity is too large"};
        }
        return capacity;
    }

    template <typename Value> [[nodiscard]] queue_status try_push_impl(Value&& event) {
        operation_guard producer{producer_gate_};
        if (!producer) {
            return queue_status::closed;
        }

        const auto index = available_indices_.try_dequeue(false);
        if (!index) {
            return queue_status::full;
        }

        try {
            std::construct_at(slots_[*index].event(), std::forward<Value>(event));
        } catch (...) {
            available_indices_.enqueue(*index, false);
            signal_capacity();
            throw;
        }

        occupied_count_.value.fetch_add(1U, std::memory_order_relaxed);
        // Publishing the index is the successful push linearization point. It
        // happens only after the Event lifetime has started in stable storage.
        published_indices_.enqueue(*index, false);
        signal_data();
        return queue_status::success;
    }

    template <typename Value>
    [[nodiscard]] queue_status wait_push_impl(Value&& event, std::stop_token stop) {
        const auto wake_waiter = [this] {
            capacity_epoch_.fetch_add(1U, std::memory_order_release);
            capacity_epoch_.notify_all();
        };
        std::stop_callback callback{stop, wake_waiter};

        while (true) {
            const auto result = try_push_impl(std::forward<Value>(event));
            if (result != queue_status::full) {
                return result;
            }
            if (stop.stop_requested()) {
                return queue_status::stopped;
            }

            const auto epoch = capacity_epoch_.load(std::memory_order_acquire);
            const auto checked_result = try_push_impl(std::forward<Value>(event));
            if (checked_result != queue_status::full) {
                return checked_result;
            }
            if (stop.stop_requested()) {
                return queue_status::stopped;
            }
            capacity_epoch_.wait(epoch, std::memory_order_acquire);
        }
    }

    template <typename Value>
    [[nodiscard]] queue_status wait_push_until_impl(Value&& event,
                                                    std::chrono::steady_clock::time_point deadline,
                                                    std::stop_token stop) {
        const auto wake_waiter = [this] { timed_waiters_.notify_all(); };
        std::stop_callback callback{stop, wake_waiter};
        std::unique_lock wait_lock{timed_wait_mutex_};

        while (true) {
            wait_lock.unlock();
            const auto result = try_push_impl(std::forward<Value>(event));
            wait_lock.lock();
            if (result != queue_status::full) {
                return result;
            }
            if (stop.stop_requested()) {
                return queue_status::stopped;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                wait_lock.unlock();
                const auto final_result = try_push_impl(std::forward<Value>(event));
                return final_result == queue_status::full ? queue_status::timeout : final_result;
            }

            const auto polling_deadline =
                std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds{1});
            timed_waiters_.wait_until(wait_lock, polling_deadline);
        }
    }

    void close_producers() {
        close_gate(producer_gate_);
        producer_quiesced_.store(true, std::memory_order_release);
        signal_data();
        signal_capacity();
    }

    static void close_gate(std::atomic<std::size_t>& gate) noexcept {
        auto state = gate.fetch_or(closed_bit, std::memory_order_acq_rel) | closed_bit;
        while ((state & active_mask) != 0U) {
            gate.wait(state, std::memory_order_acquire);
            state = gate.load(std::memory_order_acquire);
        }
    }

    void signal_data() noexcept {
        data_epoch_.fetch_add(1U, std::memory_order_release);
        data_epoch_.notify_one();
        timed_waiters_.notify_all();
    }

    void signal_capacity() noexcept {
        capacity_epoch_.fetch_add(1U, std::memory_order_release);
        capacity_epoch_.notify_one();
        timed_waiters_.notify_all();
    }

    const std::size_t capacity_;
    std::unique_ptr<slot[]> slots_;
    detail::scalable_circular_index_queue available_indices_;
    detail::scalable_circular_index_queue published_indices_;
    padded_count occupied_count_;

    std::atomic<std::size_t> producer_gate_{0U};
    std::atomic<std::size_t> consumer_gate_{0U};
    std::atomic<bool> producer_quiesced_{false};
    std::atomic<std::size_t> data_epoch_{0U};
    std::atomic<std::size_t> capacity_epoch_{0U};

    std::mutex close_mutex_;
    std::mutex timed_wait_mutex_;
    std::condition_variable timed_waiters_;
};

} // namespace event_dispatcher::queue
