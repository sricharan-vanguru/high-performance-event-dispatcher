#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace event_dispatcher {

enum class delivery_policy {
    concurrent,
    serialized,
    isolated,
};

enum class delivery_guarantee {
    best_effort,
    lossless,
};

struct subscription_options final {
    delivery_policy delivery{delivery_policy::concurrent};
    delivery_guarantee guarantee{delivery_guarantee::best_effort};
    std::size_t mailbox_capacity{64U};
    std::chrono::nanoseconds slow_callback_threshold{0};
};

struct subscription_metrics final {
    std::uint64_t offered{0U};
    std::uint64_t delivered{0U};
    std::uint64_t dropped{0U};
    std::uint64_t rejected{0U};
    std::uint64_t callback_errors{0U};
    std::uint64_t slow_callbacks{0U};
};

} // namespace event_dispatcher
