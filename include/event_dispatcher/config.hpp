#pragma once

#include <cstddef>

namespace event_dispatcher {

enum class shutdown_policy {
    drain,
    discard,
};

struct dispatcher_config final {
    std::size_t queue_capacity{4096};
    std::size_t worker_count{1};
    std::size_t worker_batch_size{1};
    shutdown_policy shutdown{shutdown_policy::drain};
    [[nodiscard]] static dispatcher_config hardware_concurrency_defaults() noexcept;
};

} // namespace event_dispatcher
