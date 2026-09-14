#pragma once

#include <string_view>

namespace event_dispatcher {

[[nodiscard]] std::string_view version() noexcept;

} // namespace event_dispatcher
