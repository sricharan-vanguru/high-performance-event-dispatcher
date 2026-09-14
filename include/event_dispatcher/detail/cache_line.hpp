#pragma once

#include <cstddef>
#include <new>

namespace event_dispatcher::detail {

inline constexpr std::size_t destructive_interference_size =
#ifdef __cpp_lib_hardware_interference_size
    std::hardware_destructive_interference_size;
#else
    64U;
#endif

} // namespace event_dispatcher::detail
