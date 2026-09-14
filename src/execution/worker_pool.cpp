#include "event_dispatcher/execution/worker_pool.hpp"

#include <exception>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace event_dispatcher::execution {

class worker_pool::implementation final {
  public:
    void start(std::size_t worker_count, const worker_function& worker) {
        if (worker_count == 0U) {
            throw std::invalid_argument{"worker_pool requires at least one worker"};
        }
        if (!worker) {
            throw std::invalid_argument{"worker_pool requires a worker function"};
        }
        if (!workers.empty()) {
            throw std::logic_error{"worker_pool can only be started once"};
        }

        workers.reserve(worker_count);
        worker_ids.reserve(worker_count);
        for (std::size_t index = 0U; index < worker_count; ++index) {
            // Every jthread owns a callable copy. If creation fails partway,
            // worker_pool destruction stops and joins the workers that exist.
            workers.emplace_back(worker, index);
            worker_ids.push_back(workers.back().get_id());
        }
    }

    void request_stop() noexcept {
        for (auto& worker : workers) {
            worker.request_stop();
        }
    }

    void join() {
        if (is_worker_thread()) {
            throw std::logic_error{"a worker cannot join its own worker_pool"};
        }
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    [[nodiscard]] bool is_worker_thread() const noexcept {
        const auto current = std::this_thread::get_id();
        for (const auto& worker_id : worker_ids) {
            if (worker_id == current) {
                return true;
            }
        }
        return false;
    }

    std::vector<std::jthread> workers;
    // IDs remain immutable after start, allowing race-free callback-side
    // self-join detection while another thread joins jthread objects.
    std::vector<std::thread::id> worker_ids;
};

worker_pool::worker_pool() : implementation_(std::make_unique<implementation>()) {}

worker_pool::worker_pool(std::size_t worker_count, worker_function worker) : worker_pool() {
    start(worker_count, std::move(worker));
}

worker_pool::~worker_pool() noexcept {
    request_stop();
    if (is_worker_thread()) {
        std::terminate();
    }
    try {
        join();
    } catch (...) {
        std::terminate();
    }
}

void worker_pool::start(std::size_t worker_count, worker_function worker) {
    implementation_->start(worker_count, worker);
}

void worker_pool::request_stop() noexcept { implementation_->request_stop(); }

void worker_pool::join() { implementation_->join(); }

bool worker_pool::is_worker_thread() const noexcept { return implementation_->is_worker_thread(); }

std::size_t worker_pool::size() const noexcept { return implementation_->workers.size(); }

} // namespace event_dispatcher::execution
