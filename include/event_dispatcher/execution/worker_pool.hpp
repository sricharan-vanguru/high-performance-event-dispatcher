#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <stop_token>

namespace event_dispatcher::execution {

// Type-independent thread ownership belongs to the compiled library. The
// private implementation keeps jthread containers out of public headers.
class worker_pool final {
  public:
    using worker_function = std::function<void(std::stop_token, std::size_t)>;

    worker_pool();
    worker_pool(std::size_t worker_count, worker_function worker);
    worker_pool(const worker_pool&) = delete;
    worker_pool& operator=(const worker_pool&) = delete;
    worker_pool(worker_pool&&) = delete;
    worker_pool& operator=(worker_pool&&) = delete;
    ~worker_pool() noexcept;

    void start(std::size_t worker_count, worker_function worker);
    void request_stop() noexcept;
    void join();
    [[nodiscard]] bool is_worker_thread() const noexcept;
    [[nodiscard]] std::size_t size() const noexcept;

  private:
    class implementation;
    std::unique_ptr<implementation> implementation_;
};

} // namespace event_dispatcher::execution
