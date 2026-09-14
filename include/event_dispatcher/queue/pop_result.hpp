#pragma once

#include "event_dispatcher/queue/queue_status.hpp"

#include <optional>
#include <type_traits>
#include <utility>

namespace event_dispatcher::queue {

// A pop must return both an outcome and, on success, the transferred event.
// Keeping them together makes invalid combinations difficult to construct at
// call sites: value() is engaged exactly when status() is success.
template <typename Event>
class pop_result final {
public:
    [[nodiscard]] static pop_result success(Event&& event) noexcept(
        std::is_nothrow_move_constructible_v<Event>) {
        return pop_result{queue_status::success, std::move(event)};
    }

    [[nodiscard]] static pop_result empty() noexcept { return pop_result{queue_status::empty}; }
    [[nodiscard]] static pop_result closed() noexcept { return pop_result{queue_status::closed}; }
    [[nodiscard]] static pop_result stopped() noexcept { return pop_result{queue_status::stopped}; }

    [[nodiscard]] queue_status status() const noexcept { return status_; }
    [[nodiscard]] bool has_value() const noexcept { return value_.has_value(); }
    [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

    [[nodiscard]] Event& value() & { return value_.value(); }
    [[nodiscard]] const Event& value() const& { return value_.value(); }
    [[nodiscard]] Event&& value() && { return std::move(value_).value(); }

private:
    explicit pop_result(queue_status status) noexcept : status_(status) {}

    pop_result(queue_status status, Event&& event) noexcept(
        std::is_nothrow_move_constructible_v<Event>)
        : status_(status), value_(std::in_place, std::move(event)) {}

    queue_status status_;
    std::optional<Event> value_;
};

} // namespace event_dispatcher::queue
