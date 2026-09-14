#include "event_dispatcher/execution/worker_pool.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stdexcept>

namespace {

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

void test_validation() {
    bool zero_rejected = false;
    try {
        event_dispatcher::execution::worker_pool pool{
            0U, [](std::stop_token, std::size_t) {}};
    } catch (const std::invalid_argument&) {
        zero_rejected = true;
    }
    expect(zero_rejected, "zero worker count was accepted");
}

void test_start_stop_and_join() {
    std::mutex mutex;
    std::condition_variable_any ready;
    std::atomic<std::size_t> started{0U};

    event_dispatcher::execution::worker_pool pool{
        4U, [&](std::stop_token stop, std::size_t) {
            started.fetch_add(1U, std::memory_order_release);
            ready.notify_all();
            std::unique_lock lock{mutex};
            ready.wait(lock, stop, [] { return false; });
        }};

    {
        std::unique_lock lock{mutex};
        ready.wait(lock, [&] { return started.load(std::memory_order_acquire) == 4U; });
    }
    expect(pool.size() == 4U, "worker_pool size mismatch");
    pool.request_stop();
    pool.join();
    pool.join();
}

} // namespace

int main() {
    test_validation();
    test_start_stop_and_join();
}
