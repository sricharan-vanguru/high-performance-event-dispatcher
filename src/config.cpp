#include "event_dispatcher/config.hpp"

#include <thread>

namespace event_dispatcher {

dispatcher_config dispatcher_config::hardware_concurrency_defaults() noexcept {
    dispatcher_config config;
    const auto detected = std::thread::hardware_concurrency();
    config.worker_count = detected == 0U ? 1U : static_cast<std::size_t>(detected);
    return config;
}

} // namespace event_dispatcher
