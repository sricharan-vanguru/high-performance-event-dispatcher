/*
 * Adapted from Ruslan Nikolaev's SCQ reference implementation, lfring_cas1.h.
 * Copyright (c) 2019 Ruslan Nikolaev. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#pragma once

#include "event_dispatcher/detail/cache_line.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>

namespace event_dispatcher::queue::detail {

// SCQ ring specialized for indices in [0, capacity). It uses 2 * capacity
// atomic entries internally, as specified by the paper. The mapping permutation
// from the reference implementation is a cache optimization; direct masking is
// equivalent for correctness and keeps this portable C++ adaptation readable.
class scalable_circular_index_queue final {
  public:
    scalable_circular_index_queue(std::size_t capacity, bool initially_full)
        : capacity_(capacity), ring_size_(capacity * 2U), ring_mask_(ring_size_ - 1U),
          entries_(std::make_unique<std::atomic<std::size_t>[]>(ring_size_)) {
        initially_full ? initialize_full() : initialize_empty();
    }

    scalable_circular_index_queue(const scalable_circular_index_queue&) = delete;
    scalable_circular_index_queue& operator=(const scalable_circular_index_queue&) = delete;

    void enqueue(std::size_t index, bool known_nonempty) noexcept {
        const auto encoded_index = index ^ ring_mask_;

        while (true) {
            const auto tail = tail_.value.fetch_add(1U, std::memory_order_acq_rel);
            const auto tail_cycle = (tail << 1U) | cycle_mask();
            auto& target = entries_[map(tail)];
            auto entry = target.load(std::memory_order_acquire);

            while (true) {
                const auto entry_cycle = entry | cycle_mask();
                const bool reusable =
                    before(entry_cycle, tail_cycle) &&
                    (entry == entry_cycle ||
                     (entry == (entry_cycle ^ ring_size_) &&
                      before_or_equal(head_.value.load(std::memory_order_acquire), tail)));
                if (!reusable) {
                    break;
                }
                if (target.compare_exchange_weak(entry, tail_cycle ^ encoded_index,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                    if (!known_nonempty &&
                        threshold_.value.load(std::memory_order_relaxed) != initial_threshold()) {
                        threshold_.value.store(initial_threshold(), std::memory_order_relaxed);
                    }
                    return;
                }
            }
        }
    }

    [[nodiscard]] std::optional<std::size_t> try_dequeue(bool known_nonempty) noexcept {
        if (!known_nonempty && threshold_.value.load(std::memory_order_relaxed) < 0) {
            return std::nullopt;
        }

        while (true) {
            const auto head = head_.value.fetch_add(1U, std::memory_order_acq_rel);
            const auto head_cycle = (head << 1U) | cycle_mask();
            auto& target = entries_[map(head)];
            std::size_t attempts = 0U;
            auto entry = target.load(std::memory_order_acquire);

            while (true) {
                const auto entry_cycle = entry | cycle_mask();
                if (entry_cycle == head_cycle) {
                    target.fetch_or(ring_mask_, std::memory_order_acq_rel);
                    return entry & ring_mask_;
                }

                std::size_t replacement = 0U;
                if ((entry | ring_size_) != entry_cycle) {
                    replacement = entry & ~ring_size_;
                    if (entry == replacement) {
                        break;
                    }
                } else {
                    ++attempts;
                    if (attempts <= catchup_attempts) {
                        entry = target.load(std::memory_order_acquire);
                        continue;
                    }
                    replacement = head_cycle ^ ((~entry) & ring_size_);
                }

                if (!before(entry_cycle, head_cycle) ||
                    target.compare_exchange_weak(entry, replacement, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                    break;
                }
            }

            if (!known_nonempty) {
                const auto tail = tail_.value.load(std::memory_order_acquire);
                if (before_or_equal(tail, head + 1U)) {
                    catch_up(tail, head + 1U);
                    threshold_.value.fetch_sub(1, std::memory_order_acq_rel);
                    return std::nullopt;
                }
                if (threshold_.value.fetch_sub(1, std::memory_order_acq_rel) <= 0) {
                    return std::nullopt;
                }
            }
        }
    }

    [[nodiscard]] bool atomics_are_lock_free() const noexcept {
        return head_.value.is_lock_free() && threshold_.value.is_lock_free() &&
               tail_.value.is_lock_free() && entries_[0].is_lock_free();
    }

  private:
    static constexpr std::size_t catchup_attempts = 10'000U;

    struct alignas(event_dispatcher::detail::destructive_interference_size) padded_index final {
        std::atomic<std::size_t> value{0U};
    };

    struct alignas(event_dispatcher::detail::destructive_interference_size) padded_threshold final {
        std::atomic<std::intptr_t> value{-1};
    };

    [[nodiscard]] std::size_t map(std::size_t index) const noexcept { return index & ring_mask_; }

    [[nodiscard]] std::size_t cycle_mask() const noexcept { return (ring_size_ * 2U) - 1U; }

    [[nodiscard]] std::intptr_t initial_threshold() const noexcept {
        return static_cast<std::intptr_t>(capacity_ + ring_size_ - 1U);
    }

    [[nodiscard]] static bool before(std::size_t left, std::size_t right) noexcept {
        return static_cast<std::intptr_t>(left - right) < 0;
    }

    [[nodiscard]] static bool before_or_equal(std::size_t left, std::size_t right) noexcept {
        return static_cast<std::intptr_t>(left - right) <= 0;
    }

    void initialize_empty() noexcept {
        for (std::size_t index = 0U; index < ring_size_; ++index) {
            entries_[index].store(std::numeric_limits<std::size_t>::max(),
                                  std::memory_order_relaxed);
        }
        head_.value.store(0U, std::memory_order_relaxed);
        threshold_.value.store(-1, std::memory_order_relaxed);
        tail_.value.store(0U, std::memory_order_relaxed);
    }

    void initialize_full() noexcept {
        for (std::size_t index = 0U; index < capacity_; ++index) {
            entries_[map(index)].store(ring_size_ + index, std::memory_order_relaxed);
        }
        for (std::size_t index = capacity_; index < ring_size_; ++index) {
            entries_[map(index)].store(std::numeric_limits<std::size_t>::max(),
                                       std::memory_order_relaxed);
        }
        head_.value.store(0U, std::memory_order_relaxed);
        threshold_.value.store(initial_threshold(), std::memory_order_relaxed);
        tail_.value.store(capacity_, std::memory_order_relaxed);
    }

    void catch_up(std::size_t tail, std::size_t head) noexcept {
        while (!tail_.value.compare_exchange_weak(tail, head, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            head = head_.value.load(std::memory_order_acquire);
            tail = tail_.value.load(std::memory_order_acquire);
            if (!before(tail, head)) {
                break;
            }
        }
    }

    const std::size_t capacity_;
    const std::size_t ring_size_;
    const std::size_t ring_mask_;
    std::unique_ptr<std::atomic<std::size_t>[]> entries_;

    padded_index head_;
    padded_threshold threshold_;
    padded_index tail_;
};

} // namespace event_dispatcher::queue::detail
