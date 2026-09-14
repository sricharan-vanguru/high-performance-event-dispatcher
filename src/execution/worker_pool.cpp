#include "event_dispatcher/execution/worker_pool.hpp"

#include <exception>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace event_dispatcher::execution {

class worker_pool::implementation final {
  public:
    class worker_scope final {
      public:
        explicit worker_scope(const implementation* owner) noexcept : previous_(current_pool_) {
            current_pool_ = owner;
        }
        worker_scope(const worker_scope&) = delete;
        worker_scope& operator=(const worker_scope&) = delete;
        ~worker_scope() { current_pool_ = previous_; }

      private:
        const implementation* previous_;
    };

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
        for (std::size_t index = 0U; index < worker_count; ++index) {
            // The thread-local scope identifies this exact pool without relying
            // on recyclable operating-system thread IDs.
            workers.emplace_back([this, worker, index](std::stop_token stop) {
                worker_scope scope{this};
                worker(stop, index);
            });
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

    [[nodiscard]] bool is_worker_thread() const noexcept { return current_pool_ == this; }

    std::vector<std::jthread> workers;

  private:
    inline static thread_local const implementation* current_pool_{nullptr};
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
