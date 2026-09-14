#pragma once

#include <cstdint>

namespace event_dispatcher {

struct dispatcher_metrics final {
    std::uint64_t accepted{0U};
    std::uint64_t rejected{0U};
    std::uint64_t dropped{0U};
};

} // namespace event_dispatcher
