#pragma once

#include "event_dispatcher/queue/pop_result.hpp"
#include "event_dispatcher/queue/queue_status.hpp"

#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <type_traits>
#include <utility>
#include <vector>

namespace event_dispatcher::queue {

// Correctness-first bounded MPMC queue.
//
// The ring storage is allocated once by the constructor. Producers and
// consumers synchronize through one mutex, which deliberately makes this the
// understandable reference implementation for the future lock-free queue.
//
// Thread-safety boundary: every method is safe to call concurrently except the
// destructor. The owner must ensure all callers have stopped before destruction.
template <typename Event>
class bounded_mutex_queue final {
public:
    static_assert(std::is_nothrow_move_constructible_v<Event>,
                  "bounded_mutex_queue requires a non-throwing Event move constructor");

    explicit bounded_mutex_queue(std::size_t capacity) : slots_(capacity) {
        if (capacity == 0U) {
            throw std::invalid_argument{"bounded_mutex_queue capacity must be greater than zero"};
        }
    }

    bounded_mutex_queue(const bounded_mutex_queue&) = delete;
    bounded_mutex_queue& operator=(const bounded_mutex_queue&) = delete;
    bounded_mutex_queue(bounded_mutex_queue&&) = delete;
    bounded_mutex_queue& operator=(bounded_mutex_queue&&) = delete;

    // A failed push never moves from event. This matters for retry loops:
    //
    //   while (queue.try_push(std::move(event)) == queue_status::full) { ... }
    //
    // The move occurs only after capacity and lifecycle checks succeed while
    // holding the queue mutex.
    [[nodiscard]] queue_status try_push(Event&& event) {
        return try_push_impl(std::move(event));
    }

    [[nodiscard]] queue_status try_push(const Event& event)
        requires std::copy_constructible<Event>
    {
        return try_push_impl(event);
    }

    // Wait until space exists, the queue closes, or cancellation is requested.
    // Immediate progress wins: if space is already available, the operation
    // succeeds even when the stop token is concurrently requested.
    [[nodiscard]] queue_status wait_push(Event&& event, std::stop_token stop = {}) {
        return wait_push_impl(std::move(event), stop);
    }

    [[nodiscard]] queue_status wait_push(const Event& event, std::stop_token stop = {})
        requires std::copy_constructible<Event>
    {
        return wait_push_impl(event, stop);
    }

    [[nodiscard]] pop_result<Event> try_pop() {
        std::unique_lock lock{mutex_};
        if (size_ == 0U) {
            return closed_ ? pop_result<Event>::closed() : pop_result<Event>::empty();
        }

        return pop_locked(lock);
    }

    // Closing does not discard accepted events. A closed queue continues to
    // return queued events, then permanently returns queue_status::closed after
    // its final item is removed.
    [[nodiscard]] pop_result<Event> wait_pop(std::stop_token stop = {}) {
        std::unique_lock lock{mutex_};

        // The stop-aware overload returns false only when cancellation wins
        // while the predicate is false. The predicate also contains closed_, so
        // close wakes a consumer even when no event remains.
        const bool ready = not_empty_.wait(lock, stop, [this] {
            return size_ != 0U || closed_;
        });

        if (!ready) {
            return pop_result<Event>::stopped();
        }
        if (size_ == 0U) {
            return pop_result<Event>::closed();
        }

        return pop_locked(lock);
    }

    // close() is idempotent. Notifications happen after releasing the mutex so
    // awakened threads do not immediately block again on the closing thread.
    void close() {
        {
            std::lock_guard lock{mutex_};
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return slots_.size(); }

    // size() and closed() are synchronized snapshots for diagnostics and tests.
    // Their return values can become stale immediately after the call; callers
    // must not use them to implement a check-then-act protocol.
    [[nodiscard]] std::size_t size() const {
        std::lock_guard lock{mutex_};
        return size_;
    }

    [[nodiscard]] bool closed() const {
        std::lock_guard lock{mutex_};
        return closed_;
    }

private:
    template <typename Value>
    [[nodiscard]] queue_status try_push_impl(Value&& event) {
        std::unique_lock lock{mutex_};
        if (closed_) {
            return queue_status::closed;
        }
        if (size_ == slots_.size()) {
            return queue_status::full;
        }

        push_locked(std::forward<Value>(event));
        lock.unlock();
        not_empty_.notify_one();
        return queue_status::success;
    }

    template <typename Value>
    [[nodiscard]] queue_status wait_push_impl(Value&& event, std::stop_token stop) {
        std::unique_lock lock{mutex_};
        const bool ready = not_full_.wait(lock, stop, [this] {
            return size_ != slots_.size() || closed_;
        });

        if (!ready) {
            return queue_status::stopped;
        }
        if (closed_) {
            return queue_status::closed;
        }

        push_locked(std::forward<Value>(event));
        lock.unlock();
        not_empty_.notify_one();
        return queue_status::success;
    }

    template <typename Value>
    void push_locked(Value&& event) {
        // The tail slot is always disengaged when size_ is below capacity.
        // optional gives each event an explicit constructed lifetime without
        // requiring Event to be default-constructible.
        slots_[tail_].emplace(std::forward<Value>(event));
        tail_ = increment(tail_);
        ++size_;
    }

    [[nodiscard]] pop_result<Event> pop_locked(std::unique_lock<std::mutex>& lock) {
        // Event is required to have a non-throwing move constructor. This makes
        // ownership transfer lossless: after we remove the occupied slot, no
        // later Event move can fail and strand an accepted event.
        Event event{std::move(*slots_[head_])};
        slots_[head_].reset();
        head_ = increment(head_);
        --size_;

        lock.unlock();
        not_full_.notify_one();
        return pop_result<Event>::success(std::move(event));
    }

    [[nodiscard]] std::size_t increment(std::size_t index) const noexcept {
        ++index;
        return index == slots_.size() ? 0U : index;
    }

    std::vector<std::optional<Event>> slots_;

    mutable std::mutex mutex_;
    std::condition_variable_any not_empty_;
    std::condition_variable_any not_full_;

    std::size_t head_{0U};
    std::size_t tail_{0U};
    std::size_t size_{0U};
    bool closed_{false};
};

} // namespace event_dispatcher::queue
