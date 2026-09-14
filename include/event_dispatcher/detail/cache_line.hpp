#pragma once

#include <cstddef>

namespace event_dispatcher::detail {

// Keep public template layout stable across compiler versions and -mtune flags.
// Sixty-four bytes is the supported baseline; platform-specific tuning can be
// introduced later without silently changing installed-header ABI.
inline constexpr std::size_t destructive_interference_size = 64U;

} // namespace event_dispatcher::detail
