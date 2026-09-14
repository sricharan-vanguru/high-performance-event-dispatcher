#pragma once

#include <cstddef>
#include <thread>

namespace event_dispatcher {

enum class shutdown_policy {
    drain,
    discard,
};

enum class callback_concurrency {
    concurrent,
    serialized,
};

struct dispatcher_config final {
    std::size_t queue_capacity{4096};
    std::size_t worker_count{1};
    shutdown_policy shutdown{shutdown_policy::drain};
    callback_concurrency callback_mode{callback_concurrency::concurrent};

    [[nodiscard]] static dispatcher_config hardware_concurrency_defaults() noexcept {
        dispatcher_config config;
        const auto detected = std::thread::hardware_concurrency();
        config.worker_count = detected == 0U ? 1U : static_cast<std::size_t>(detected);
        return config;
    }
};

} // namespace event_dispatcher
