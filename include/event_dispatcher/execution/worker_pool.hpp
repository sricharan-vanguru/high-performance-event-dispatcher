#pragma once

#include <cstddef>
#include <exception>
#include <functional>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <utility>
#include <vector>

namespace event_dispatcher::execution {

// A small ownership wrapper around std::jthread. It intentionally knows
// nothing about events or queues: the caller supplies the complete worker loop.
class worker_pool final {
public:
    using worker_function = std::function<void(std::stop_token, std::size_t)>;

    worker_pool() = default;

    worker_pool(std::size_t worker_count, worker_function worker) {
        start(worker_count, std::move(worker));
    }

    worker_pool(const worker_pool&) = delete;
    worker_pool& operator=(const worker_pool&) = delete;
    worker_pool(worker_pool&&) = delete;
    worker_pool& operator=(worker_pool&&) = delete;

    ~worker_pool() noexcept {
        request_stop();
        join_noexcept();
    }

    void start(std::size_t worker_count, worker_function worker) {
        if (worker_count == 0U) {
            throw std::invalid_argument{"worker_pool requires at least one worker"};
        }
        if (!worker) {
            throw std::invalid_argument{"worker_pool requires a worker function"};
        }
        if (!workers_.empty()) {
            throw std::logic_error{"worker_pool can only be started once"};
        }

        workers_.reserve(worker_count);
        worker_ids_.reserve(worker_count);
        for (std::size_t index = 0U; index < worker_count; ++index) {
            // Each jthread owns its callable, so copying std::function here is
            // deliberate. If thread creation fails partway through, already
            // constructed jthreads request stop and join during unwinding.
            workers_.emplace_back(worker, index);
            worker_ids_.push_back(workers_.back().get_id());
        }
    }

    void request_stop() noexcept {
        for (auto& worker : workers_) {
            worker.request_stop();
        }
    }

    void join() {
        if (is_worker_thread()) {
            throw std::logic_error{"a worker cannot join its own worker_pool"};
        }
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    [[nodiscard]] bool is_worker_thread() const noexcept {
        const auto current = std::this_thread::get_id();
        for (const auto& worker_id : worker_ids_) {
            if (worker_id == current) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::size_t size() const noexcept { return workers_.size(); }

private:
    void join_noexcept() noexcept {
        if (is_worker_thread()) {
            // Destroying an owner from one of its own workers violates the
            // documented lifetime contract. Termination is preferable to a
            // silent self-join deadlock or detached access to dead state.
            std::terminate();
        }
        try {
            join();
        } catch (...) {
            std::terminate();
        }
    }

    std::vector<std::jthread> workers_;
    // IDs never change after start, so callback-side self-join detection stays
    // race-free while another thread is joining the jthread objects.
    std::vector<std::thread::id> worker_ids_;
};

} // namespace event_dispatcher::execution
